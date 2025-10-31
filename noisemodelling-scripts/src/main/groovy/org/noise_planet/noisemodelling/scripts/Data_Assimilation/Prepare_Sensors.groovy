package org.noise_planet.noisemodelling.scripts.Data_Assimilation

import com.opencsv.CSVReader
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicInteger

title = 'Preparation of Sensor data'
description = 'Extraction of sensor data for a given period and creation of sql tables '

inputs = [
        startDate: [
                name: 'Start Time Stamp',
                title: 'Start Time Stamp',
                description: 'the start timestamp to extract the dataset in format "%Y-%m-%d %H:%M:%S" ',
                type: String.class
        ],
        endDate: [
                name: 'End Time Stamp',
                title: 'End Time Stamp',
                description: 'the end timestamp to extract the dataset in format "%Y-%m-%d %H:%M:%S" ',
                type: String.class
        ],
        trainingRatio: [
                name: 'Training data percentage',
                title: 'Training data percentage',
                description: 'Training data as a percentage of total data ',
                type: Float.class
        ],
        workingFolder: [
                name: 'Input folder path',
                title: 'Working directory path with input files',
                description: 'Folder containing csv files "device_mapping_sf", the osm file and the folder "devices_data"',
                type: String.class
        ],
        targetSRID : [
                name       : 'Target projection identifier',
                title      : 'Target projection identifier',
                description: '&#127757; Target projection identifier (also called SRID) of your table.<br>' +
                        'It should be an <a href="https://epsg.io/" target="_blank">EPSG</a> code, an integer with 4 or 5 digits (ex: <a href="https://epsg.io/3857" target="_blank">3857</a> is Web Mercator projection).<br><br>' +
                        '&#x1F6A8; The target SRID must be in <b>metric</b> coordinates example 2056 for Geneva.',
                type       : Integer.class
        ]
]

outputs = [
        result: [
                name: 'Sql tables output',
                title: 'Sql tables output',
                description: 'Sql tables "SENSORS_MEASUREMENTS", "SENSORS_LOCATION", "SENSORS_MEASUREMENTS_TRAINING"',
                type: String.class
        ]
]


static def exec(Connection connection,input){
    connection = new ConnectionWrapper(connection)
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info('Start Preparation of Sensor dataset ')

    Sql sql = new Sql(connection)
    boolean isPostgis = DatabaseHelper.isPostgreSQL(connection)
    String sensorsLocationTable = DatabaseHelper.normalizeTableName(connection, 'SENSORS_LOCATION')
    String sensorsMeasurementsTable = DatabaseHelper.normalizeTableName(connection, 'SENSORS_MEASUREMENTS')
    String sensorsTrainingTable = DatabaseHelper.normalizeTableName(connection, 'SENSORS_MEASUREMENTS_TRAINING')
    String geomColumn = DatabaseHelper.normalizeColumnName(connection, 'THE_GEOM')
    String folderPath = input['workingFolder']
    if (!folderPath.endsWith("/")) folderPath += "/"

    String deviceFolder = folderPath + "devices_data"
    LocalDateTime dayStart = parseTimestamp(input['startDate'] as String)
    LocalDateTime dayEnd = parseTimestamp(input['endDate'] as String)
    Float trainingRatio = input['trainingRatio'] as Float
    Integer targetSRID = input['targetSRID'] as Integer

    List<Map<String, String>> selectedData = allMeasurements(dayStart, dayEnd,deviceFolder)
    measurementTable(connection,selectedData)

    boolean columnExists = false
    String schemaClause = isPostgis ? " AND table_schema = current_schema()" : " AND TABLE_SCHEMA = 'PUBLIC'"
    String columnCheckQuery = "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_NAME) = ? AND LOWER(COLUMN_NAME) = 'deveui'" + schemaClause
    sql.eachRow(columnCheckQuery, [sensorsLocationTable.toLowerCase()]) { row ->
        if (row.cnt > 0) {
            columnExists = true
        }
    }

    if (columnExists) {
        sql.execute("ALTER TABLE " + sensorsLocationTable + " RENAME COLUMN deveui TO idsensor")
    }

    if (isPostgis) {
        sql.execute("ALTER TABLE " + sensorsLocationTable + " ALTER COLUMN " + geomColumn + " TYPE geometry(PointZ, " + targetSRID + ") USING ST_SetSRID(ST_Force3D(" + geomColumn + "), " + targetSRID + ")")
    } else {
        sql.execute("UPDATE " + sensorsLocationTable + " SET " + geomColumn + " = ST_SetSRID(ST_Force3D(" + geomColumn + "), " + targetSRID + ") WHERE " + geomColumn + " IS NOT NULL")
    }
    DatabaseHelper.ensureSRID(connection, sensorsLocationTable, geomColumn, targetSRID)

    // Create the RECEIVERS table with unique sensor data from measurement (SENSORS_MEASUREMENTS_TRAINING: training data) table.
    DatabaseHelper.addAutoIncrementPrimaryKey(connection, sensorsLocationTable, 'PK')

    String measurementColumnCheck = "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_NAME) = ? AND LOWER(COLUMN_NAME) = ?" + schemaClause
    Closure<Integer> columnCount = { String tableName, String columnName ->
        def row = sql.firstRow(measurementColumnCheck, [tableName.toLowerCase(), columnName.toLowerCase()])
        return (row?.cnt ?: 0) as int
    }
    boolean measurementHasGeom = columnCount(sensorsMeasurementsTable, geomColumn) > 0
    if (!measurementHasGeom) {
        if (isPostgis) {
            sql.execute("ALTER TABLE " + sensorsMeasurementsTable + " ADD COLUMN " + geomColumn + " geometry(PointZ, " + targetSRID + ")")
        } else {
            sql.execute("ALTER TABLE " + sensorsMeasurementsTable + " ADD COLUMN " + geomColumn + " GEOMETRY")
        }
    }
    DatabaseHelper.ensureSRID(connection, sensorsMeasurementsTable, geomColumn, targetSRID)
    if (columnCount(sensorsMeasurementsTable, 'idreceiver') == 0) {
        sql.execute("ALTER TABLE " + sensorsMeasurementsTable + " ADD COLUMN idreceiver INTEGER")
    }

    sql.execute("UPDATE SENSORS_MEASUREMENTS sm " +
            "SET THE_GEOM = (select ST_Transform(s.The_GEOM, "+targetSRID+" )"+
            " FROM SENSORS_LOCATION s " +
            " WHERE sm.IDSENSOR = s.IDSENSOR );")

    sql.execute("UPDATE SENSORS_MEASUREMENTS sm " +
            "SET IDRECEIVER = (select PK"+
            " FROM SENSORS_LOCATION s " +
            " WHERE sm.IDSENSOR = s.IDSENSOR);")

    extractObservationData(connection,trainingRatio)

    if (isPostgis) {
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN " + geomColumn + " TYPE geometry(PointZ, " + targetSRID + ") USING ST_SetSRID(ST_Force3D(" + geomColumn + "), " + targetSRID + ")")
    } else {
        sql.execute("UPDATE " + sensorsTrainingTable + " SET " + geomColumn + " = ST_SetSRID(ST_Force3D(" + geomColumn + "), " + targetSRID + ") WHERE " + geomColumn + " IS NOT NULL")
    }
    DatabaseHelper.ensureSRID(connection, sensorsTrainingTable, geomColumn, targetSRID)

    if (isPostgis) {
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN epoch TYPE INTEGER USING epoch::integer")
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN idreceiver TYPE INTEGER USING idreceiver::integer")
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN temp TYPE DOUBLE PRECISION USING temp::double precision")
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN laeq TYPE DOUBLE PRECISION USING laeq::double precision")
    } else {
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN epoch SET DATA TYPE INTEGER")
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN idreceiver SET DATA TYPE INTEGER")
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN temp SET DATA TYPE FLOAT")
        sql.execute("ALTER TABLE " + sensorsTrainingTable + " ALTER COLUMN laeq SET DATA TYPE FLOAT")
    }

    logger.info('End Preparation of Sensor dataset ')
    return "Calculation Done ! The tables SENSORS_MEASUREMENTS, SENSORS_LOCATION and SENSORS_MEASUREMENTS_TRAINING have been created."



}
/**
 * Extracts sensor data from the sensor measurements files for a given period dayStart -> dayEnd
 *
 * @param dayStart: Starting date period
 * @param dayEnd: Ending date period
 * @param deviceFolder: Path to the folder containing all the measurements of sensors
 * @return a Map containing all sensors data
 */
static def allMeasurements(LocalDateTime dayStart, LocalDateTime dayEnd, String deviceFolder) {
    List<Map<String, String>> selectedData = []

    Files.walk(Paths.get(deviceFolder))
            .filter { Files.isRegularFile(it) && it.toString().endsWith(".csv") }
            .forEach { path ->
                try {
                    CSVReader reader = new CSVReader(new FileReader(path.toFile()))
                    List<String[]> rows = reader.readAll()
                    String[] headers = rows.get(0)
                    for (int i = 1; i < rows.size(); i++) {
                        String[] row = rows.get(i)
                        Map<String, String> map = [:]

                        for (int j = 0; j < headers.length; j++) {
                            if (j < row.length) {
                                map[headers[j]] = row[j]
                            }
                         }

                        map["file_name"] = path.getFileName().toString()

                        if (map.containsKey("timestamp") && map["timestamp"]) {
                           LocalDateTime ts = parseTimestamp(map["timestamp"])
                           if (!ts.isBefore(dayStart) && !ts.isAfter(dayEnd)) {
                              selectedData.add(map)
                           }
                        }
                  }

                } catch (Exception e) {
                    e.printStackTrace()
                }
            }

    return selectedData
}



/**
 * Extracts sensor data from the sensor measurements files for a given period dayStart -> dayEnd
 *
 * @param connection: The database connection used for executing queries.
 * @param selectedData Map containing all sensors
 */
@CompileStatic
static def measurementTable(Connection connection,List<Map<String, String>> selectedData) {
    Sql sql = new Sql(connection)
    sql.execute("DROP TABLE IF EXISTS SENSORS_MEASUREMENTS;")
    sql.execute("CREATE TABLE SENSORS_MEASUREMENTS (" +
            "    deveui VARCHAR(255)," +
            "    EPOCH VARCHAR(255)," +
            "    Leq FLOAT," +
            "    Temp FLOAT" +
            ")")

    String tableName = "SENSORS_MEASUREMENTS"

    List<String> columns = ["deveui", "epoch", "Leq", "Temp"]

    selectedData.each { row ->
        StringBuilder sb = new StringBuilder()
        sb.append("INSERT INTO ").append(tableName).append(" (")
                .append(columns.join(", ")).append(") VALUES (")

        columns.eachWithIndex { col, i ->
            String value = row.getOrDefault(col, "")
            if (col == "Leq" || col == "Temp") {
                sb.append(value)
            } else {
                sb.append("'").append(value.replace("'", "''")).append("'")
            }
            if (i < columns.size() - 1) sb.append(", ")
        }

        sb.append(")")
        sql.execute(sb.toString())
    }
    sql.execute("ALTER TABLE SENSORS_MEASUREMENTS RENAME COLUMN deveui TO IDSENSOR ")
    sql.execute("ALTER TABLE SENSORS_MEASUREMENTS RENAME COLUMN Leq TO LAEQ ")
}

/**
 * Converts a timestamp to LocalDateTime.
 * Supports: milliseconds (as string), ISO format (yyyy-MM-dd'T'HH:mm:ss), space format (yyyy-MM-dd HH:mm:ss).
 *
 * @param timestamp Timestamp as string (milliseconds or date format)
 * @return Corresponding LocalDateTime
 */
@CompileStatic
static def parseTimestamp(String timestamp) {
    // Check if it's numeric (milliseconds)
    if (timestamp.isNumber()) {
        try {
            long millis = Long.parseLong(timestamp)
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault())
        } catch (NumberFormatException e) {
            throw e
        }
    }

    try {
        // First try ISO format with 'T'
        DateTimeFormatter isoFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
        return LocalDateTime.parse(timestamp, isoFormatter)
    } catch (DateTimeParseException e1) {
        try {
            // If that fails, try format with space
            DateTimeFormatter spaceFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            return LocalDateTime.parse(timestamp, spaceFormatter)
        } catch (DateTimeParseException e2) {
            throw e2
        }
    }
}


/**
 * Extracts training observation sensor data from an input CSV one hour file and writes filtered training dataset to an output CSV file.
 *
 * @param connection  : The database connection used for executing queries.
 * @param ratio : the percentage of the training data
 */
static def extractObservationData(Connection connection,Float ratio) {
    Sql sql = new Sql(connection)

    List<Map<String, Object>> measureRows = sql.rows("SELECT * FROM SENSORS_MEASUREMENTS")

    Map<String, Integer> idReceiverMap = [:]
    AtomicInteger idCounter = new AtomicInteger(1)

    Set<String> uniqueSensors = measureRows*.IDSENSOR.toSet() as Set<String>

    uniqueSensors.each { sensor ->
        idReceiverMap[sensor] = idCounter.getAndIncrement()
    }

    int keepSize = (int) (idReceiverMap.size() * ratio)

    def shuffledEntries = idReceiverMap.entrySet().toList()
    Random random = new Random(42)  // Set seed for reproducibility
    Collections.shuffle(shuffledEntries, random)

    Map<String, Integer> resultMap = shuffledEntries.subList(0, keepSize).collectEntries {
        [(it.key): it.value]
    }
    sql.execute("DROP TABLE IF EXISTS SENSORS_MEASUREMENTS_TRAINING;")
    sql.execute("""
        CREATE TABLE IF NOT EXISTS SENSORS_MEASUREMENTS_TRAINING (
            IDSENSOR VARCHAR(255),
            THE_GEOM VARCHAR(255),
            IDRECEIVER INTEGER,
            EPOCH INTEGER,
            LAEQ FLOAT,
            TEMP FLOAT
        )
    """)

    measureRows.each { row ->
        String sensor = row.IDSENSOR
        if (resultMap.containsKey(sensor)) {
            sql.execute("INSERT INTO SENSORS_MEASUREMENTS_TRAINING (IDSENSOR, THE_GEOM, IDRECEIVER, EPOCH, LAEQ, TEMP) " +
                    " VALUES ('${sensor}', '${row.The_GEOM}', ${row.IDRECEIVER}, '${row.EPOCH}', ${row.LAEQ}, ${row.Temp})")
        }
    }



}
