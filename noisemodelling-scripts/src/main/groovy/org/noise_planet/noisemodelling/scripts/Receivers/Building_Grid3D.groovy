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
import java.sql.PreparedStatement

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

    boolean isPostgreSQL = DatabaseHelper.isPostgreSQL(connection)

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
    String buildingHeightAlias = DatabaseHelper.normalizeColumnName(connection, "BUILDING_HEIGHT")

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
    DatabaseHelper.dropTableIfExists(connection, receivers_table_name)

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
    String buildingPk = DatabaseHelper.getPrimaryKeyColumn(connection, building_table_name)
    buildingPk = DatabaseHelper.normalizeColumnName(connection, buildingPk)
    logger.info('The input building table has a Primary Key named ' + buildingPk)
    if (buildingPk == "" || !buildingPk) {
        return "To run this script, your input Buildings table must have a Primary Key."
    }

    DatabaseHelper.dropTableIfExists(connection, 'tmp_receivers_lines')
    def filter_geom_query = ""
    Map<String, Object> queryParams = [distance_wall: distance]

    if (fenceGeom != null) {
        DatabaseHelper.GeometryParameter fenceParam = DatabaseHelper.prepareGeometryParameter(connection, fenceGeom, 'fenceGeom')
        filter_geom_query = " WHERE " + geomColumn + " && " + fenceParam.expression +
                " AND ST_INTERSECTS(" + geomColumn + ", " + fenceParam.expression + ")"
        queryParams.putAll(fenceParam.parameters)
    }
    
    // Database-specific function for converting buffer boundary to lines
    String toMultiLineFunc = isPostgreSQL ? "ST_Boundary" : "ST_ToMultiLine"
    
    // create line of receivers
    sql.execute("CREATE TABLE tmp_receivers_lines as SELECT " + buildingPk + " as pk_building, " +
                "ST_SimplifyPreserveTopology(" + toMultiLineFunc + "(ST_Buffer(" + geomColumn + ", :distance_wall, 'join=bevel')), 0.05) " + geomColumn + ", " + heightColumn + " " +
                "FROM " + building_table_name + filter_geom_query, queryParams)
    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, 'tmp_receivers_lines', geomColumn)

    // union of truncated receivers and non truncated, split line to points
    DatabaseHelper.dropTableIfExists(connection, 'TMP_SCREENS_MERGE')
    sql.execute("CREATE TABLE TMP_SCREENS_MERGE AS SELECT s." + geomColumn + " AS " + geomColumn + ", " +
            "s." + heightColumn + " AS " + buildingHeightAlias + ", s.pk_building FROM tmp_receivers_lines s " +
            "WHERE NOT ST_IsEmpty(s." + geomColumn + ")")
    DatabaseHelper.addAutoIncrementPrimaryKey(connection, 'TMP_SCREENS_MERGE', 'pk')

    // Collect all lines and convert into points using custom method
    DatabaseHelper.dropTableIfExists(connection, 'TMP_SCREENS')
    
    // H2GIS supports geometry(PointZ, SRID) just like PostgreSQL
    String tmpScreensGeomDefinition = DatabaseHelper.geometryColumnDefinition(connection, geomColumn, "PointZ", targetSrid)
    sql.execute("CREATE TABLE TMP_SCREENS(pk integer, " + tmpScreensGeomDefinition + ", level int, pk_building int)")
    
    // Prepare cross-database INSERT statement using helper
    Map<String, Object> insertInfo = DatabaseHelper.prepare3DPointInsertStatement(connection, 'TMP_SCREENS', 
                                                                                   geomColumn, targetSrid, 
                                                                                   ['pk', 'level', 'pk_building'])
    String insertQuery = insertInfo.query as String
    
    GeometryFactory factory = new GeometryFactory(new PrecisionModel(), targetSrid)
    connection.prepareStatement(insertQuery).withCloseable { PreparedStatement ps ->
        // Cross-database geometry handling without SpatialResultSet unwrap
        def stmt = connection.createStatement()
        def rs = stmt.executeQuery("SELECT * FROM TMP_SCREENS_MERGE")

        String mergeGeomColumn = DatabaseHelper.normalizeResultSetColumnName(connection, geomColumn)
        String mergeHeightColumn = DatabaseHelper.normalizeResultSetColumnName(connection, buildingHeightAlias)
        String mergePkColumn = DatabaseHelper.normalizeResultSetColumnName(connection, 'pk')
        String mergePkBuildingColumn = DatabaseHelper.normalizeResultSetColumnName(connection, 'pk_building')

        while (rs.next()) {
            List<Coordinate> pts = new ArrayList<Coordinate>()
            Geometry geom = DatabaseHelper.getGeometryFromResultSet(connection, rs, geomColumn)
            int pk = rs.getInt(mergePkColumn)
            double hBuilding = rs.getDouble(mergeHeightColumn)
            int pk_building = rs.getInt(mergePkBuildingColumn)
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
                            Coordinate newCoord = new Coordinate(pt.x, pt.y, 1.5+i*h)
                            // Use helper to add batch - handles PG vs H2GIS internally
                            DatabaseHelper.addBatch3DPoint(connection, ps, newCoord, factory, [pk, i, pk_building])
                        }
                    }
                }
            }
        }
        rs.close()
        stmt.close()
        ps.executeBatch()
    }
    DatabaseHelper.dropTableIfExists(connection, 'TMP_SCREENS_MERGE')
    DatabaseHelper.dropTableIfExists(connection, receivers_table_name)


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
            DatabaseHelper.GeometryParameter fenceParam = DatabaseHelper.prepareGeometryParameter(connection, fenceGeom, 'fenceFilter')
            sql.execute("DELETE FROM " + receivers_table_name + " g WHERE not ST_INTERSECTS(g." + geomColumn + " , " + fenceParam.expression + ");", fenceParam.parameters)
        }
    } else {
        // buildings have population attribute
        // set population attribute divided by number of receiver to each receiver

        logger.info('Create RECEIVERS table...')

        DatabaseHelper.dropTableIfExists(connection, 'tmp_receivers')
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
            DatabaseHelper.GeometryParameter fenceParam = DatabaseHelper.prepareGeometryParameter(connection, fenceGeom, 'fenceFilterTmp')
            sql.execute("DELETE FROM tmp_receivers g WHERE not ST_INTERSECTS(g." + geomColumn + " , " + fenceParam.expression + ");", fenceParam.parameters)
        }
    sql.execute("CREATE INDEX ON tmp_receivers(build_pk)")
    sql.execute("CREATE TABLE " + receivers_table_name + "(pk serial, " + geomColumn + " geometry, level integer, pop float, pk_building integer)")
    sql.execute("INSERT INTO " + receivers_table_name + " (" + geomColumn + ", level, pop, pk_building) " +
            "SELECT a." + geomColumn + ", a.level, b.pop/COUNT(DISTINCT aa.pk)::float, a.pk_building " +
            "FROM tmp_receivers a, " + building_table_name + " b,tmp_receivers aa " +
            "WHERE b." + buildingPk + " = a.pk_building AND a.build_pk = aa.build_pk " +
            "GROUP BY a." + geomColumn + ", a.build_pk, a.level, b.pop, a.pk_building")
    DatabaseHelper.dropTableIfExists(connection, 'tmp_receivers')
    }

    logger.info("Delete receivers inside buildings")
    sql.execute("DELETE FROM " + receivers_table_name + " g WHERE exists " +
                    "(SELECT 1 FROM " + building_table_name + " b " +
                        "WHERE ST_Z(g." + geomColumn + ") < b." + heightColumn + " and g." + geomColumn + " && b." + geomColumn + " and ST_INTERSECTS(g." + geomColumn + ", b." + geomColumn + ") limit 1);")


    // cleaning
    DatabaseHelper.dropTablesIfExist(connection,
        'TMP_SCREENS',
        'tmp_screen_truncated',
        'tmp_relation_screen_building',
        'tmp_receivers_lines',
        'tmp_buildings')
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
