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


package org.noise_planet.noisemodelling.scripts.Geometric_Tools

import groovy.sql.Sql
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.utilities.dbtypes.DBUtils
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection

title = 'Clean BUILDINGS Table'
description = '&#10145;&#65039; Clean the BUILDINGS table, avoiding overlapping areas and unclosed polygons.' +
              '<hr>' +
              'NoiseModelling propagation code does not support well intersecting polygons </br> </br>' +
              '&#x2705;  The input table will be erased and replaced by the cleaned one.'

inputs = [
        tableName: [
                name       : 'Buildings table name',
                title      : 'Buildings table name',
                description: '<b>Name of the Buildings table.</b> </br> </br>' +
                             'The table must be projected in a metric coordinate system (SRID). Use "Change_SRID" WPS Block if needed. </br> </br>' +
                             'The table shall contain: </br>' +
                             '- <b> THE_GEOM </b>: the 2D geometry of the building (POLYGON or MULTIPOLYGON).</br>' +
                             '- <b> HEIGHT </b>: the height of the building (FLOAT)',
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

// main function of the script
def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null

    // Get DBTypes for proper table/column name handling
    DBTypes dbType = DBUtils.getDBType(connection)

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Clean buildings table')
    logger.info("inputs {}", input) // log inputs of the run

    // -------------------
    // Get every inputs
    // -------------------

    // Get table name and parse into TableLocation object
    String building_table_name = TableLocation.capsIdentifier(input['tableName'] as String, dbType)
    TableLocation buildingTableLoc = TableLocation.parse(building_table_name, dbType)

    //get SRID of the table
    int srid = GeometryTableUtilities.getSRID(connection, buildingTableLoc)
    
    // If metadata SRID is 0, try to get SRID from actual geometry data
    if (srid == 0) {
        def row = sql.firstRow("SELECT ST_SRID(THE_GEOM) as srid FROM " + buildingTableLoc + " LIMIT 1")
        if (row != null && row.srid != null) {
            srid = row.srid as Integer
        }
    }
    
    if (srid == 3785 || srid == 4326) throw new IllegalArgumentException("Error : This SRID is not metric. Please use another SRID for your table.")
    if (srid == 0) throw new IllegalArgumentException("Error : The table does not have an associated SRID.")


    Boolean hasPop = JDBCUtilities.hasField(connection, buildingTableLoc, "POP")
    String popField = ""
    String popFieldDef = ""
    if (hasPop) {
        logger.info("The building table has a column named POP.")
        popField = ", POP"
        popFieldDef = ", POP REAL"
    }
    if (!hasPop) {
        logger.info("The building table has not a column named POP.")
    }

    // -------------------------
    // Initialize some variables
    // -------------------------

    // Database-specific precision reduction function - use direct connection check
    boolean isPostgreSQL = DatabaseHelper.isPostgreSQL(connection)
    logger.info("Database type: " + (isPostgreSQL ? "PostgreSQL" : "H2GIS"))
    
    String precisionFunc
    if (isPostgreSQL) {
        // PostgreSQL uses ST_SnapToGrid(geom, size) - grid size of 0.01 for 2 decimal places
        precisionFunc = "ST_SnapToGrid(THE_GEOM, 0.01)"
        logger.info("Using PostgreSQL precision function: ST_SnapToGrid")
    } else {
        // H2GIS uses ST_PrecisionReducer(geom, decimals)
        precisionFunc = "ST_PrecisionReducer(THE_GEOM, 2)"
        logger.info("Using H2GIS precision function: ST_PrecisionReducer")
    }

    sql.execute('drop table if exists buildings_temp;' +
            'create table buildings_temp as select ST_MAKEVALID(ST_SIMPLIFYPRESERVETOPOLOGY(' + precisionFunc + ',0.1)) THE_GEOM, PK, HEIGHT '+popField+' from '+building_table_name+'  WHERE ST_Perimeter(THE_GEOM)<1000;')

    logger.info('Make valid every buildings - ok')

    // Database-specific ALTER TABLE syntax
    if (isPostgreSQL) {
        sql.execute("ALTER TABLE buildings_temp ALTER COLUMN PK TYPE INTEGER")
        sql.execute("ALTER TABLE buildings_temp ALTER COLUMN PK SET NOT NULL")
    } else {
        sql.execute("ALTER TABLE buildings_temp ALTER COLUMN PK INT NOT NULL;")
    }
    sql.execute("ALTER TABLE buildings_temp ADD PRIMARY KEY (PK); ")
    
    // Create spatial index - database specific syntax
    if (isPostgreSQL) {
        sql.execute('CREATE INDEX ON buildings_temp USING GIST(the_geom);')
    } else {
        sql.execute('CREATE SPATIAL INDEX ON buildings_temp(the_geom);')
    }
    
    sql.execute('drop table if exists tmp_relation_buildings;' +
            'create table tmp_relation_buildings as select s1.PK as PK_BUILDING, S2.PK as PK2_BUILDING FROM buildings_temp S1, buildings_temp S2 WHERE ST_AREA(S1.THE_GEOM) < ST_AREA(S2.THE_GEOM) AND S1.THE_GEOM && S2.THE_GEOM AND ST_DISTANCE(S1.THE_GEOM, S2.THE_GEOM) <= 0.1;')

    logger.info('Intersection founded')

    String fieldPopS1 = "";
    if(hasPop) {
        fieldPopS1 = ", s1.POP"
    }
    
    // Database-specific geometry aggregation function and GROUP BY clause
    String geomAccumFunc = isPostgreSQL ? "ST_Union" : "ST_ACCUM"
    String groupByClause = isPostgreSQL ? "PK_BUILDING, s1.the_geom, s1.HEIGHT" + (hasPop ? ", s1.POP" : "") : "PK_BUILDING"
    
    sql.execute("CREATE INDEX ON tmp_relation_buildings(PK_BUILDING);" +
            "drop table if exists tmp_buildings_truncated;" +
            "create table tmp_buildings_truncated as select PK_BUILDING, ST_DIFFERENCE(s1.the_geom,  ST_BUFFER(${geomAccumFunc}(s2.the_geom), 0.1, 'join=mitre')) the_geom, s1.HEIGHT "+fieldPopS1+" from tmp_relation_buildings r, buildings_temp s1, buildings_temp s2 WHERE PK_BUILDING = S1.PK  AND PK2_BUILDING = S2.PK   GROUP BY " + groupByClause + ";")

    logger.info('Intersection remove buildings with intersections')

    sql.execute("DROP TABLE IF EXISTS "+building_table_name+";")
    
    // Database-specific CREATE TABLE syntax
    if (isPostgreSQL) {
        // PostgreSQL: CREATE TABLE AS SELECT, then add PRIMARY KEY
        sql.execute("CREATE TABLE "+building_table_name+" AS SELECT s.PK, ST_SetSRID(s.the_geom,"+srid+") AS THE_GEOM, s.HEIGHT "+popField+" FROM BUILDINGS_TEMP s WHERE PK NOT IN (SELECT PK_BUILDING FROM tmp_buildings_truncated) UNION ALL SELECT PK_BUILDING AS PK, ST_SetSRID(the_geom,"+srid+") AS THE_GEOM, HEIGHT "+popField+" FROM tmp_buildings_truncated WHERE NOT st_isempty(the_geom);")
        sql.execute("ALTER TABLE "+building_table_name+" ADD PRIMARY KEY (PK);")
    } else {
        // H2GIS: CREATE TABLE with column definitions and AS SELECT
        sql.execute("CREATE TABLE "+building_table_name+"(PK INTEGER PRIMARY KEY, THE_GEOM GEOMETRY, HEIGHT FLOAT "+popFieldDef+") AS SELECT s.PK, ST_SetSRID(s.the_geom,"+srid+"), s.HEIGHT "+popField+" FROM BUILDINGS_TEMP s WHERE PK NOT IN (SELECT PK_BUILDING FROM tmp_buildings_truncated) UNION ALL SELECT PK_BUILDING, ST_SetSRID(the_geom,"+srid+"), HEIGHT "+popField+" FROM tmp_buildings_truncated WHERE NOT st_isempty(the_geom);")
    }
    
    // Register geometry metadata - database specific
    if (isPostgreSQL) {
        // For PostgreSQL, use DatabaseHelper.ensureSRID to properly update geometry_columns
        logger.info("Registering SRID for PostgreSQL using DatabaseHelper")
        DatabaseHelper.ensureSRID(connection, building_table_name, 'THE_GEOM', srid)
        // Verify SRID was registered
        String tableLoc = TableLocation.parse(building_table_name, dbType).toString()
        def verifyResult = sql.firstRow("SELECT ST_SRID(THE_GEOM) as srid FROM " + tableLoc + " LIMIT 1")
        logger.info("SRID verification: " + verifyResult?.srid)
    } else {
        // For H2GIS, use UpdateGeometrySRID
        sql.execute("SELECT UpdateGeometrySRID($building_table_name,'THE_GEOM',$srid);")
    }
    
    sql.execute("DELETE FROM "+building_table_name+" WHERE ST_ISEMPTY(THE_GEOM)")
    logger.info('Create spatial index on new building table')
    
    // Create spatial index - database specific syntax
    if (isPostgreSQL) {
        sql.execute('CREATE INDEX ON ' + building_table_name + ' USING GIST(the_geom);')
    } else {
        sql.execute('CREATE SPATIAL INDEX ON '+building_table_name+'(the_geom);')
    }
    
    sql.execute("drop table if exists tmp_buildings_truncated;")
    sql.execute('drop table if exists tmp_relation_buildings;')
    sql.execute('drop table if exists buildings_temp;')
    resultString = resultString + "Calculation Done !"

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : Clean buildings table')

    // print to WPS Builder
    return resultString

}
