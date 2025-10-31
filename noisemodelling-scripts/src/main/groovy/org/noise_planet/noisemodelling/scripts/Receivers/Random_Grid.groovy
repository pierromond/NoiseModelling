/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
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
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection

title = 'Random Grid'
description = '&#10145;&#65039; Computes a random grid of receivers.</br>' +
              '<hr>' +
              '&#x2705; The output table is called <b>RECEIVERS</b> </br></br>'+
              '<img src="/wps_images/receivers_random_output.png" alt="Random grid output" width="95%" align="center">'

inputs = [
        buildingTableName: [
                name       : 'Buildings table name',
                title      : 'Buildings table name',
                description: 'Name of the Buildings table </br> </br>' +
                             'The table must contain: <ul>' +
                             '<li> <b>THE_GEOM</b>: the 2D geometry of the building (POLYGON or MULTIPOLYGON)</li>' +
                             '<li> <b>HEIGHT</b>: the height of the building (FLOAT)</li></ul>',
                type       : String.class
        ],
        sourcesTableName : [
                name       : 'Sources table name',
                title      : 'Sources table name',
                description: 'Keep only receivers at least at 1 meters of provided sources geometries </br> </br>' +
                             'The table must contain : <ul>' +
                             '<li> <b>THE_GEOM</b>: any geometry type. </li></ul>',
                type       : String.class
        ],
        nReceivers       : [
                name       : 'Number of receivers',
                title      : 'Number of receivers',
                description: 'Number of receivers to return </br> </br>' +
                             '&#128736; Default value: <b>100</b> </br> </br>'+
                             '<img src="/wps_images/receivers_random_nReceivers.png" alt="Number of receivers" width="95%" align="center">',
                type       : Integer.class
        ],
        height           : [
                name :       'Height',
                title:       'Height', 
                description: 'Height of receivers (in meters) (FLOAT)</br> </br>' +
                             '&#128736; Default value: <b>4 </b> ',
                min  : 0, max: 1,
                type : Double.class
        ],
        fence            : [
                name       : 'Fence geometry',
                title      : 'Extent filter',
                description: 'Create receivers only in the provided polygon. </br> </br>' +
                             'Must be in the WGS84 (EPSG:4326) projection system',
                min        : 0, max: 1,
                type       : Geometry.class
        ],
        fenceTableName   : [
                name       : 'Fence geometry from table',
                title      : 'Filter using table bounding box',
                description: 'Extract the bounding box of the specified table then create only receivers on the table bounding box. </br> </br>' +
                             'The table must contain : <ul>' +
                             '<li> <b>THE_GEOM</b>: any geometry type. </li></ul>',
                min        : 0, max: 1,
                type       : String.class
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
    logger.info('Start : Random grid')
    logger.info("inputs {}", input) // log inputs of the run


    String receivers_table_name = "RECEIVERS"

    Integer nReceivers = 100
    if (input['nReceivers']) {
        nReceivers = input['nReceivers'] as Integer
    }

    Double h = 4.0d
    if (input['height']) {
        h = input['height'] as Double
    }

    String sources_table_name = "SOURCES"
    if (input['sourcesTableName']) {
        sources_table_name = input['sourcesTableName']
    }


    String building_table_name = input['buildingTableName']
    
    // Get database-specific column names
    String geomColumn = DatabaseHelper.getGeometryColumnName(connection)
    String heightColumn = DatabaseHelper.normalizeColumnName(connection, "HEIGHT")

    Sql sql = new Sql(connection)

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
        fenceGeom = DatabaseHelper.getTableEnvelope(connection, fenceTableName, geomColumn)
    }


    //Delete previous receivers grid...
    sql.execute(String.format("DROP TABLE IF EXISTS %s", receivers_table_name))

    def filter_geom_query = ""

    Envelope envelope
    if (fenceGeom == null) {
        envelope = DatabaseHelper.getTableEnvelope(connection, sources_table_name, geomColumn).getEnvelopeInternal();
        envelope.expandToInclude(DatabaseHelper.getTableEnvelope(connection, building_table_name, geomColumn).getEnvelopeInternal())
    } else {
        envelope = fenceGeom.envelopeInternal
    }

    // Cross-database compatible range function and random function
    String rangeFunction = DatabaseHelper.isPostgreSQL(connection) ? "generate_series(0," + nReceivers.toString() + ")" : "system_range(0," + nReceivers.toString() + ")"
    String randomFunction = DatabaseHelper.isPostgreSQL(connection) ? "RANDOM()" : "RAND()"

    sql.execute("create table " + receivers_table_name + " as select ST_SetSRID(ST_MAKEPOINT(" + randomFunction + "*(" + envelope.maxX + " - " + envelope.minX.toString() + ") + " + envelope.minX.toString() + ", " + randomFunction + "*(" + envelope.maxY.toString() + " - " + envelope.minY.toString() + ") + " + envelope.minY.toString() + ", " + h + "), " + targetSrid.toInteger() + ") as " + geomColumn + " from " + rangeFunction + ";")


    if (input['fence']) {
        // Delete points outside geom but inside
        DatabaseHelper.GeometryParameter fenceParam = DatabaseHelper.prepareGeometryParameter(connection, fenceGeom, 'fenceGeom')
        sql.execute("DELETE FROM " + receivers_table_name + " WHERE NOT ST_Intersects(" + geomColumn + ", " + fenceParam.expression + ")", 
                   fenceParam.parameters)
    }

    logger.info("Create spatial index on " + receivers_table_name)
    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, receivers_table_name, geomColumn)

    logger.info('Delete receivers where buildings...')
    if (input['buildingTableName']) {
        //Delete receivers inside buildings .
        sql.execute("delete from " + receivers_table_name + " g where exists (select 1 from " + building_table_name + " b where g." + geomColumn + " && b." + geomColumn + " and ST_distance(b." + geomColumn + ", g." + geomColumn + ") < 1 and b." + heightColumn + " >= " + h + " limit 1);")
    }

    logger.info('Delete receivers where sound sources...')
    if (input['sourcesTableName']) {
        //Delete receivers near sources
        sql.execute("delete from " + receivers_table_name + " g where exists (select 1 from " + sources_table_name + " r where st_expand(g." + geomColumn + ", 1) && r." + geomColumn + " and st_distance(g." + geomColumn + ", r." + geomColumn + ") < 1 limit 1);")
    }

    logger.info('Add Primary Key column...')
    DatabaseHelper.addAutoIncrementPrimaryKey(connection, receivers_table_name, 'pk')

    // Process Done
    resultString = "Process done. Table of receivers " + receivers_table_name + " created !"

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : Random grid')


    // print to WPS Builder
    // Ensure SRID is properly set for PostgreSQL (fixes PostGIS metadata)
    // Pass the explicit SRID since receiver generation may create geometries with SRID=0
    DatabaseHelper.ensureSRID(connection, receivers_table_name, 'the_geom', targetSrid)

    return resultString

}
