/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team FROM the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

/**
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */


package org.noise_planet.noisemodelling.scripts.Receivers

import groovy.sql.Sql
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.SpatialResultSet
import org.h2gis.utilities.dbtypes.DBUtils
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.WKBReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection

title = 'Buildings Grid'
description = '&#10145;&#65039; Generates 3D receivers around the buildings and at different levels.</br>' +
              '<hr>' +
              'Main parameters: </br><ul>' +
              '<li>"Height between levels": coupled with the building height, allows to determine the number of levels,</li>' +
              '<li>"Distance from wall": set the distance between the receivers and the building facades,</li>'+
              '<li>"Distance between receivers": set the number of receivers around the buildings.</li></ul></br>' +
              '&#x2705; The output table is called <b>RECEIVERS</b> </br></br>'+
              '<img src="/wps_images/Building_Grid3D.png" alt="Building grid output" width="95%" align="center">'

inputs = [
        tableBuilding   : [
                name       : 'Buildings table name',
                title      : 'Buildings table name',
                description: 'Name of the Buildings table. <br></br>' +
                             'The table must contain: <ul>' +
                             '<li> <b>THE_GEOM</b> : the 2D geometry of the building (POLYGON or MULTIPOLYGON)</li>' +
                             '<li> <b>HEIGHT</b> : the height of the building (in meter) (FLOAT)</li>' +
                             '<li> <b>POP</b> : building population to add in the receiver attribute (FLOAT) (Optionnal)</li></ul>',
                type       : String.class
        ],
        fence           : [
                name       : 'Fence geometry',
                title      : 'Extent filter',
                description: 'Create receivers only in the provided polygon (fence)',
                min        : 0, max: 1,
                type       : Geometry.class
        ],
        fenceTableName  : [
                name       : 'Fence geometry from table',
                title      : 'Filter using table bounding box',
                description: 'Filter receivers, using the bounding box of the given table name:<br><ol>' +
                             '<li> Extract the bounding box of the specified table,</li>' +
                             '<li> then create only receivers on the table bounding box.</li></ol>' +
                             'The given table must contain: <ul>' +
                             '<li> <b>THE_GEOM</b> : any geometry type. </li></ul>',
                min        : 0, max: 1,
                type       : String.class
        ],
        sourcesTableName: [
                name       : 'Sources table name',
                title      : 'Sources table name',
                description: 'Keep only receivers that are at least 1 meter from the provided source geometries.</br></br>' +
                             'The source geometries table must contain: <ul>' +
                             '<li> <b>THE_GEOM</b> : any geometry type. </li></ul>',
                min        : 0, max: 1,
                type       : String.class
        ],
        delta           : [
                name       : 'Receivers minimal distance',
                title      : 'Distance between receivers',
                description: 'Distance between receivers (in the Cartesian plane - in meters) (FLOAT) </br></br>'+
                             '&#128736; Default value: <b>10 </b>',
                type       : Double.class
        ],
        heightLevels          : [
                name       : 'Height between levels',
                title      : 'Height between levels',
                description: 'Height between each level of receivers (in meters) (FLOAT) </br> </br>' +
                             '&#128736; Default value: <b>2.5 </b> ',
                min        : 0, max: 1,
                type       : Double.class
        ],
        distance          : [
                name       : 'Distance',
                title      : 'Distance from wall',
                description: 'Distance of receivers from the wall (in meters) (FLOAT) </br></br>' +
                             '&#128736; Default value: <b>2 </b>',
                min        : 0, max: 1,
                type       : Double.class
        ]
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]




def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null

   // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : 3D Receivers grid around buildings')
    logger.info("inputs {}", input) // log inputs of the run

    String receivers_table_name = DatabaseHelper.normalizeTableName(connection, "RECEIVERS")

    Double delta = 10
    if (input['delta']) {
        delta = input['delta'] as Double
    }

    Double h = 2.5d
    if (input['heightLevels']) {
        h = input['heightLevels'] as Double
    }

    Double distance = 2.0d
    if (input['distance']) {
        distance = input['distance'] as Double
    }


    String sources_table_name = "SOURCES"
    if (input['sourcesTableName']) {
        sources_table_name = input['sourcesTableName']
    }


    String building_table_name = input['tableBuilding']
    
    // Get database-specific column names
    String geomColumn = DatabaseHelper.getGeometryColumnName(connection)
    String heightColumn = DatabaseHelper.normalizeColumnName(connection, "HEIGHT")

    Boolean hasPop = JDBCUtilities.hasField(connection, building_table_name, "POP")
    if (hasPop) logger.info("The building table has a column named POP.")
    if (!hasPop) logger.info("The building table has not a column named POP.")

    if (!JDBCUtilities.hasField(connection, building_table_name, heightColumn)) {
        resultString = "To run this script, your input Buildings table must have column named HEIGHT."
    // Ensure SRID is properly set for PostgreSQL (fixes PostGIS metadata)
    DatabaseHelper.ensureSRID(connection, receivers_table_name, 'the_geom')

        return resultString
    }

    //Statement sql = connection.createStatement()
    Sql sql = new Sql(connection)
    sql.execute(String.format("DROP TABLE IF EXISTS %s", receivers_table_name))

    // Normalize table names for H2GIS utility calls
    String building_table_name_for_utils = DatabaseHelper.normalizeTableNameForUtilities(connection, building_table_name)
    String sources_table_name_for_utils = DatabaseHelper.normalizeTableNameForUtilities(connection, sources_table_name)

    // Reproject fence - use DatabaseHelper for PostgreSQL compatibility
    int targetSrid = DatabaseHelper.getTableSRID(connection, building_table_name, geomColumn)
    if (targetSrid == 0 && input['sourcesTableName']) {
        targetSrid = DatabaseHelper.getTableSRID(connection, sources_table_name, geomColumn)
    }

    Geometry fenceGeom = null
    if (input['fence']) {
        if (targetSrid != 0) {
            // Transform fence to the same coordinate system than the buildings & sources
            WKTReader wktReader = new WKTReader()
            fence = wktReader.read(input['fence'] as String)
            fenceGeom = ST_Transform.ST_Transform(connection, ST_SetSRID.setSRID(fence, 4326), targetSrid)
        } else {
            throw new Exception("Unable to find buildings or sources SRID, ignore fence parameters")
        }
    } else if (input['fenceTableName']) {
        String fenceTableName = input['fenceTableName'] as String
        String fenceTableName_for_utils = DatabaseHelper.normalizeTableNameForUtilities(connection, fenceTableName)
        fenceGeom = DatabaseHelper.getTableEnvelope(connection, fenceTableName, geomColumn)
    }


    // Get primary key column name with PostgreSQL workaround
    String buildingPk = ""
    if (DatabaseHelper.isPostgreSQL(connection)) {
        // PostgreSQL workaround: query pg_index directly due to case sensitivity issue in JDBCUtilities
        TableLocation buildingTable = TableLocation.parse(building_table_name_for_utils)
        def row = sql.firstRow("""
            SELECT a.attname 
            FROM pg_index i 
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) 
            WHERE i.indrelid = ?::regclass AND i.indisprimary
        """, [buildingTable.toString()])
        if (row) {
            buildingPk = row.attname
        }
    } else {
        buildingPk = JDBCUtilities.getColumnName(connection, building_table_name_for_utils,
                JDBCUtilities.getIntegerPrimaryKey(connection,
                        TableLocation.parse(building_table_name_for_utils, DBUtils.getDBType(connection))))
    }
    logger.info('The input building table has a Primary Key named ' + buildingPk)
    if (buildingPk == "" || !buildingPk) {
        return "To run this script, your input Buildings table must have a Primary Key."
    }

    sql.execute("DROP TABLE IF EXISTS tmp_receivers_lines")
    def filter_geom_query = ""
    def queryParams = [distance_wall: distance]
    
    if (fenceGeom != null) {
        if (DatabaseHelper.isPostgreSQL(connection)) {
            // PostgreSQL: convert geometry to WKT for prepared statement
            String wkt = fenceGeom.toString()
            int srid = fenceGeom.getSRID()
            filter_geom_query = " WHERE " + geomColumn + " && ST_SetSRID(ST_GeomFromText(:fenceGeomWkt), :fenceGeomSRID) " +
                               "AND ST_INTERSECTS(" + geomColumn + ", ST_SetSRID(ST_GeomFromText(:fenceGeomWkt), :fenceGeomSRID))"
            queryParams.fenceGeomWkt = wkt
            queryParams.fenceGeomSRID = srid
        } else {
            // H2GIS: accepts Geometry directly
            filter_geom_query = " WHERE " + geomColumn + " && :fenceGeom AND ST_INTERSECTS(" + geomColumn + ", :fenceGeom)"
            queryParams.fenceGeom = fenceGeom
        }
    }
    
    // Database-specific function for converting buffer boundary to lines
    String toMultiLineFunc = DatabaseHelper.isPostgreSQL(connection) ? "ST_Boundary" : "ST_ToMultiLine"
    
    // create line of receivers
    sql.execute("CREATE TABLE tmp_receivers_lines as SELECT " + buildingPk + " as pk_building, " +
                "ST_SimplifyPreserveTopology(" + toMultiLineFunc + "(ST_Buffer(" + geomColumn + ", :distance_wall, 'join=bevel')), 0.05) " + geomColumn + ", " + heightColumn + " " +
                "FROM " + building_table_name + filter_geom_query, queryParams)
    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, 'tmp_receivers_lines', geomColumn)

    // union of truncated receivers and non tructated, split line to points
    sql.execute("DROP TABLE IF EXISTS TMP_SCREENS_MERGE")
    
    // PostgreSQL doesn't support CREATE TABLE (columns) AS SELECT
    if (DatabaseHelper.isPostgreSQL(connection)) {
        sql.execute("CREATE TABLE TMP_SCREENS_MERGE as SELECT s." + geomColumn + ", s." + heightColumn + ", s.pk_building FROM tmp_receivers_lines s WHERE not st_isempty(s." + geomColumn + ") ;")
        // Rename columns if needed and add primary key
        sql.execute("ALTER TABLE TMP_SCREENS_MERGE RENAME COLUMN " + heightColumn + " TO hBuilding")
        sql.execute("ALTER TABLE TMP_SCREENS_MERGE ADD COLUMN PK SERIAL PRIMARY KEY")
    } else {
        sql.execute("CREATE TABLE TMP_SCREENS_MERGE (" + geomColumn + " geometry, hBuilding float, pk_building integer) as SELECT s." + geomColumn + ", s." + heightColumn + ", s.pk_building FROM tmp_receivers_lines s WHERE not st_isempty(s." + geomColumn + ") ;")
        sql.execute("ALTER TABLE TMP_SCREENS_MERGE ADD COLUMN PK SERIAL PRIMARY KEY")
    }

    // Collect all lines and convert into points using custom method
    sql.execute("DROP TABLE IF EXISTS TMP_SCREENS")
    
    // PostgreSQL needs explicit geometry type with SRID in column definition
    // Use PointZ for 3D coordinates
    if (DatabaseHelper.isPostgreSQL(connection)) {
        sql.execute("CREATE TABLE TMP_SCREENS(pk integer, " + geomColumn + " geometry(PointZ, " + targetSrid + "), level int, pk_building int)")
    } else {
        sql.execute("CREATE TABLE TMP_SCREENS(pk integer, " + geomColumn + " geometry, level int, pk_building int)")
    }
    
    // Database-specific INSERT statement for geometry
    def qry
    if (DatabaseHelper.isPostgreSQL(connection)) {
        // Use ST_MakePoint(x, y, z) for 3D points instead of ST_GeomFromText
        qry = 'INSERT INTO TMP_SCREENS(pk, ' + geomColumn + ', level, pk_building) VALUES (?, ST_SetSRID(ST_MakePoint(?, ?, ?), ' + targetSrid + '), ?, ?);'
    } else {
        qry = 'INSERT INTO TMP_SCREENS(pk, ' + geomColumn + ', level, pk_building) VALUES (?,?,?,?);'
    }
    
    GeometryFactory factory = new GeometryFactory(new PrecisionModel(), targetSrid);
    sql.withBatch(100, qry) { ps ->
        // Cross-database geometry handling without SpatialResultSet unwrap
        def stmt = connection.createStatement()
        def rs = stmt.executeQuery("SELECT * FROM TMP_SCREENS_MERGE")
        
        // PostgreSQL normalizes column names to lowercase
        String geomColName = DatabaseHelper.isPostgreSQL(connection) ? geomColumn.toLowerCase() : geomColumn
        WKBReader wkbReader = DatabaseHelper.isPostgreSQL(connection) ? new WKBReader() : null
        
        while (rs.next()) {
            List<Coordinate> pts = new ArrayList<Coordinate>()
            // Get geometry - use cross-database approach
            Geometry geom
            if (DatabaseHelper.isPostgreSQL(connection)) {
                def pgGeom = rs.getObject(geomColName)
                byte[] wkb = org.postgresql.util.PGobject.class.cast(pgGeom).getValue().decodeHex()
                geom = wkbReader.read(wkb)
            } else {
                // H2GIS returns Geometry directly from getObject
                geom = rs.getObject(geomColName) as Geometry
            }
            int pk = rs.getInt("pk")
            def hBuilding = rs.getDouble("hBuilding")
            def pk_building = rs.getInt("pk_building")
            if (geom instanceof LineString) {
                splitLineStringIntoPoints(geom as LineString, delta, pts)
            } else if (geom instanceof MultiLineString) {
                for (int idgeom = 0; idgeom < geom.numGeometries; idgeom++) {
                    splitLineStringIntoPoints(geom.getGeometryN(idgeom) as LineString, delta, pts)
                }
            }
            int nLevels = Math.ceil((hBuilding-1.5)/h)
            if (hBuilding>1.5){
                for (int i=0;i<nLevels;i++){
                    for (int idp = 0; idp < pts.size(); idp++) {
                        Coordinate pt = pts.get(idp);
                        if (!Double.isNaN(pt.x) && !Double.isNaN(pt.y)) {
                            // define coordinates of receivers
                            Coordinate newCoord = new Coordinate(pt.x, pt.y, 1.5+i*h)
                            Point point = factory.createPoint(newCoord)
                            
                            // For PostgreSQL, INSERT uses ST_MakePoint(x, y, z) so we pass coordinates separately
                            // For H2GIS, we pass the Geometry object directly
                            if (DatabaseHelper.isPostgreSQL(connection)) {
                                ps.addBatch(pk, newCoord.x, newCoord.y, newCoord.z, i, pk_building)
                            } else {
                                ps.addBatch(pk, point, i, pk_building)
                            }
                        }
                    }
                }
            }
        }
        rs.close()
        stmt.close()
    }
    sql.execute("DROP TABLE IF EXISTS TMP_SCREENS_MERGE")
    sql.execute("DROP TABLE IF EXISTS " + receivers_table_name)


    if (!hasPop) {
        // buildings have no population attribute
        logger.info('Create RECEIVERS table...')

        sql.execute("CREATE TABLE " + receivers_table_name + "(pk serial, " + geomColumn + " geometry, level integer, pk_building integer);")
        sql.execute("INSERT INTO " + receivers_table_name + " (" + geomColumn + ", level, pk_building) " +
                        "SELECT ST_SetSRID(" + geomColumn + "," + targetSrid.toInteger() + "), level, pk_building FROM TMP_SCREENS;")
        // Create spatial index (cross-database compatible)
        DatabaseHelper.createSpatialIndex(connection, receivers_table_name, geomColumn)

        if (input['sourcesTableName']) {
            // Delete receivers near sources
            logger.info('Delete receivers near sources...')
            // Create spatial index (cross-database compatible)
            DatabaseHelper.createSpatialIndex(connection, sources_table_name, geomColumn)
            sql.execute("DELETE FROM " + receivers_table_name + " g WHERE exists  " +
                            "(SELECT 1 FROM " + sources_table_name + " r  " +
                                "WHERE st_expand(g." + geomColumn + ", 1, 1) && r." + geomColumn + " and st_distance(g." + geomColumn + ", r." + geomColumn + ") < 1 limit 1);")
        }

        if (fenceGeom != null) {
            // Delete receiver not in fence filter
            logger.info('Delete receivers that are not in the fence')
            if (DatabaseHelper.isPostgreSQL(connection)) {
                // PostgreSQL: convert geometry to WKT for prepared statement
                String fenceWkt = fenceGeom.toString()
                int fenceSrid = fenceGeom.getSRID()
                sql.execute("DELETE FROM " + receivers_table_name + " g WHERE not ST_INTERSECTS(g." + geomColumn + " , ST_SetSRID(ST_GeomFromText(:fenceWkt), :fenceSrid));", 
                    [fenceWkt: fenceWkt, fenceSrid: fenceSrid])
            } else {
                // H2GIS: accepts Geometry directly
                sql.execute("DELETE FROM " + receivers_table_name + " g WHERE not ST_INTERSECTS(g." + geomColumn + " , :fenceGeom);", [fenceGeom : fenceGeom])
            }
        }
    } else {
        // buildings have population attribute
        // set population attribute divided by number of receiver to each receiver

        logger.info('Create RECEIVERS table...')
        
        sql.execute("DROP TABLE IF EXISTS tmp_receivers")
        sql.execute("CREATE TABLE tmp_receivers(" + geomColumn + " geometry, build_pk integer, level integer, pk_building integer)")
        sql.execute("ALTER TABLE tmp_receivers ADD COLUMN PK SERIAL PRIMARY KEY")// Ajout Gwen
        sql.execute("INSERT INTO tmp_receivers(" + geomColumn + ", build_pk, level, pk_building) " +
                        "SELECT ST_SetSRID(" + geomColumn + "," + targetSrid.toInteger() + "), pk, level, pk_building FROM TMP_SCREENS;")

        if (input['sourcesTableName']) {
            // Delete receivers near sources
            logger.info('Delete receivers near sources...')
            // Create spatial index (cross-database compatible)
            DatabaseHelper.createSpatialIndex(connection, sources_table_name, geomColumn)
            sql.execute("DELETE FROM tmp_receivers g WHERE exists " +
                            "(SELECT 1 FROM " + sources_table_name + " r " +
                                "WHERE st_expand(g." + geomColumn + ", 1, 1) && r." + geomColumn + " and st_distance(g." + geomColumn + ", r." + geomColumn + ") < 1 limit 1);")
        }

        if (fenceGeom != null) {
            // Delete receiver not in fence filter
            logger.info('Delete receivers that are not in the fence')
            sql.execute("DELETE FROM tmp_receivers g WHERE not ST_INTERSECTS(g." + geomColumn + " , " +
                    "ST_SETSRID(ST_GeomFromText('" + fenceGeom + "'), "+targetSrid.toInteger()+"));")
        }

        sql.execute("CREATE INDEX ON tmp_receivers(build_pk)")
        sql.execute("CREATE TABLE " + receivers_table_name + "(pk serial, " + geomColumn + " geometry, level integer, pop float, pk_building integer)")
        sql.execute("INSERT INTO " + receivers_table_name + " (" + geomColumn + ", level, pop, pk_building) " +
                        "SELECT a." + geomColumn + ", a.level, b.pop/COUNT(DISTINCT aa.pk)::float, a.pk_building " +
                        "FROM tmp_receivers a, " + building_table_name + " b,tmp_receivers aa " +
                        "WHERE b." + buildingPk + " = a.pk_building AND a.build_pk = aa.build_pk " +
                        "GROUP BY a." + geomColumn + ", a.build_pk, a.level, b.pop, a.pk_building")
        sql.execute("DROP TABLE IF EXISTS tmp_receivers")
    }

    logger.info("Delete receivers inside buildings")
    sql.execute("DELETE FROM " + receivers_table_name + " g WHERE exists " +
                    "(SELECT 1 FROM " + building_table_name + " b " +
                        "WHERE ST_Z(g." + geomColumn + ") < b." + heightColumn + " and g." + geomColumn + " && b." + geomColumn + " and ST_INTERSECTS(g." + geomColumn + ", b." + geomColumn + ") limit 1);")


    // cleaning
    String dropSyntax = DatabaseHelper.isPostgreSQL(connection) ? "DROP TABLE IF EXISTS" : "drop table"
    String dropSuffix = DatabaseHelper.isPostgreSQL(connection) ? "" : " if exists"
    sql.execute(dropSyntax + " TMP_SCREENS" + dropSuffix)
    sql.execute(dropSyntax + " tmp_screen_truncated" + dropSuffix)
    sql.execute(dropSyntax + " tmp_relation_screen_building" + dropSuffix)
    sql.execute(dropSyntax + " tmp_receivers_lines" + dropSuffix)
    sql.execute(dropSyntax + " tmp_buildings" + dropSuffix)
    // Process Done
    resultString = "Process done. The receivers table named " + receivers_table_name + " has been created!"

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : 3D Receivers grid around buildings')

    // print to WPS Builder
    // Ensure SRID is properly set for PostgreSQL (fixes PostGIS metadata)
    DatabaseHelper.ensureSRID(connection, receivers_table_name, 'the_geom')

    return resultString

}


/**
 *
 * @param geom Geometry
 * @param segmentSizeConstraint Maximal distance between points
 * @param [out]pts computed points
 * @return Fixed distance between points
 */
double splitLineStringIntoPoints(LineString geom, double segmentSizeConstraint,
                                 List<Coordinate> pts) {
    // If the linear sound source length is inferior than half the distance between the nearest point of the sound
    // source and the receiver then it can be modelled as a single point source
    double geomLength = geom.getLength();
    if (geomLength < segmentSizeConstraint) {
        // Return mid point
        Coordinate[] points = geom.getCoordinates();
        double segmentLength = 0;
        final double targetSegmentSize = geomLength / 2.0;
        for (int i = 0; i < points.length - 1; i++) {
            Coordinate a = points[i];
            final Coordinate b = points[i + 1];
            double length = a.distance3D(b);
            if (length + segmentLength > targetSegmentSize) {
                double segmentLengthFraction = (targetSegmentSize - segmentLength) / length;
                Coordinate midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                        a.y + segmentLengthFraction * (b.y - a.y),
                        a.z + segmentLengthFraction * (b.z - a.z));
                pts.add(midPoint);
                break;
            }
            segmentLength += length;
        }
        return geom.getLength();
    } else {
        double targetSegmentSize = geomLength / Math.ceil(geomLength / segmentSizeConstraint);
        Coordinate[] points = geom.getCoordinates();
        double segmentLength = 0.0;

        // Mid point of segmented line source
        def midPoint = null;
        for (int i = 0; i < points.length - 1; i++) {
            Coordinate a = points[i];
            final Coordinate b = points[i + 1];
            double length = a.distance3D(b);
            if (Double.isNaN(length)) {
                length = a.distance(b);
            }
            while (length + segmentLength > targetSegmentSize) {
                //LineSegment segment = new LineSegment(a, b);
                double segmentLengthFraction = (targetSegmentSize - segmentLength) / length;
                Coordinate splitPoint = new Coordinate();
                splitPoint.x = a.x + segmentLengthFraction * (b.x - a.x);
                splitPoint.y = a.y + segmentLengthFraction * (b.y - a.y);
                splitPoint.z = a.z + segmentLengthFraction * (b.z - a.z);
                if (midPoint == null && length + segmentLength > targetSegmentSize / 2) {
                    segmentLengthFraction = (targetSegmentSize / 2.0 - segmentLength) / length;
                    midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                            a.y + segmentLengthFraction * (b.y - a.y),
                            a.z + segmentLengthFraction * (b.z - a.z));
                }
                pts.add(midPoint);
                a = splitPoint;
                length = a.distance3D(b);
                if (Double.isNaN(length)) {
                    length = a.distance(b);
                }
                segmentLength = 0;
                midPoint = null;
            }
            if (midPoint == null && length + segmentLength > targetSegmentSize / 2) {
                double segmentLengthFraction = (targetSegmentSize / 2.0 - segmentLength) / length;
                midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                        a.y + segmentLengthFraction * (b.y - a.y),
                        a.z + segmentLengthFraction * (b.z - a.z));
            }
            segmentLength += length;
        }
        if (midPoint != null) {
            pts.add(midPoint);
        }
        return targetSegmentSize;
    }
}
