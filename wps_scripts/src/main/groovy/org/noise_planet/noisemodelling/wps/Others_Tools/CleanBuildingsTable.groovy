/**
 * NoiseModelling is a free and open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by Université Gustave Eiffel and CNRS
 * <http://noise-planet.org/noisemodelling.html>
 * as part of:
 * the Eval-PDU project (ANR-08-VILL-0005) 2008-2011, funded by the Agence Nationale de la Recherche (French)
 * the CENSE project (ANR-16-CE22-0012) 2017-2021, funded by the Agence Nationale de la Recherche (French)
 * the Nature4cities (N4C) project, funded by European Union’s Horizon 2020 research and innovation programme under grant agreement No 730468
 *
 * Noisemap is distributed under GPL 3 license.
 *
 * Contact: contact@noise-planet.org
 *
 * Copyright (C) 2011-2012 IRSTV (FR CNRS 2488) and Ifsttar
 * Copyright (C) 2013-2019 Ifsttar and CNRS
 * Copyright (C) 2020 Université Gustave Eiffel and CNRS
 *
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */

package org.noise_planet.noisemodelling.wps.Others_Tools

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.functions.io.osm.OSMRead
import org.locationtech.jts.geom.Geometry
import org.h2gis.utilities.wrapper.ConnectionWrapper

import java.sql.Connection
import java.sql.Statement

title = 'Clean Buildings Table'
description = 'Clean Buildings Table. ' +
        ' <br>Avoid overlapping areas.' 

inputs = [tableBuilding     : [name       : 'Buildings table name', title: 'Buildings table name',
                               description: '<b>Name of the Buildings table.</b>  </br>  ' +
                                       '<br>  The table shall contain : </br>' +
                                       '- <b> THE_GEOM </b> : the 2D geometry of the building (POLYGON or MULTIPOLYGON). </br>' +
                                       '- <b> HEIGHT </b> : the height of the building (FLOAT)',
                               type       : String.class]]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'This type of result does not allow the blocks to be linked together.', type: String.class]]

// Open Connection to Geoserver
static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}

// run the script
def run(input) {

    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}

// main function of the script
def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null

    // print to command window
    System.out.println('Start : Clean Buildings table')
    def start = new Date()

    // -------------------
    // Get every inputs
    // -------------------

      // import building_table_name
    String building_table_name =  input['tableBuilding'] as String
    
    // do it case-insensitive
    tableName = building_table_name.toUpperCase()



    // -------------------------
    // Initialize some variables
    // -------------------------

   
   // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)
    connection = new ConnectionWrapper(connection)

    int srid = 0000
    // get the PrimaryKey field if exists to keep it in the final table
    int pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, tableName)

    // Build the result string with every tables
    StringBuilder sbFields = new StringBuilder()
    // Get the column names to keep all column in the final table
    List<String> fields = JDBCUtilities.getFieldNames(connection.getMetaData(), tableName)
    int k = 1
    String pkField = ""
    fields.each {
        f ->
            if (f != "THE_GEOM") {
                sbFields.append(String.format(" , %s ", f))
            }
            if (pkIndex == k) pkField = f.toString()
            k++
    }

        //get SRID of the table
    srid = SFSUtilities.getSRID(connection, TableLocation.parse(tableName))

sql.execute('drop table if exists buildings_temp;'+
            'create table buildings_temp as select ST_MAKEVALID(ST_precisionreducer(ST_SIMPLIFYPRESERVETOPOLOGY(THE_GEOM,0.1),1)) THE_GEOM, PK, HEIGHT from buildings  WHERE ST_Perimeter(THE_GEOM)<1000;')


    System.out.println('Make valid ok')

    sql.execute("ALTER TABLE buildings_temp ALTER COLUMN PK INT NOT NULL;")
    sql.execute("ALTER TABLE buildings_temp ADD PRIMARY KEY (PK); ")
    sql.execute('CREATE SPATIAL INDEX IF NOT EXISTS BUILDINGS_INDEX ON buildings_temp(the_geom);'+
            'drop table if exists tmp_relation_buildings;'+
            'create table tmp_relation_buildings as select s1.PK as PK_BUILDING, S2.PK as PK2_BUILDING FROM buildings_temp S1, buildings_temp S2 WHERE ST_AREA(S1.THE_GEOM) < ST_AREA(S2.THE_GEOM) AND S1.THE_GEOM && S2.THE_GEOM AND ST_DISTANCE(S1.THE_GEOM, S2.THE_GEOM) <= 0.1;')

    System.out.println('Intersection founded')

    sql.execute("CREATE INDEX ON tmp_relation_buildings(PK_BUILDING);"+
            "drop table if exists tmp_buildings_truncated;" +
            "create table tmp_buildings_truncated as select PK_BUILDING, ST_DIFFERENCE(s1.the_geom,  ST_BUFFER(ST_ACCUM(s2.the_geom), 0.1, 'join=mitre')) the_geom from tmp_relation_buildings r, buildings_temp s1, buildings_temp s2 WHERE PK_BUILDING = S1.PK  AND PK2_BUILDING = S2.PK   GROUP BY PK_BUILDING;")

    System.out.println('Intersection tmp_buildings_truncated')
    sql.execute("DROP TABLE IF EXISTS BUILDINGS;")
    sql.execute("create table BUILDINGS(PK INTEGER PRIMARY KEY, THE_GEOM GEOMETRY, HEIGHT FLOAT)  as select s.PK, s.the_geom, s.HEIGHT from  BUILDINGS_TEMP s where PK not in (select PK_BUILDING from tmp_buildings_truncated) UNION ALL select PK_BUILDING, the_geom from tmp_buildings_truncated WHERE NOT st_isempty(the_geom);")

    sql.execute("drop table if exists tmp_buildings_truncated;")

    resultString = resultString + "Calculation Done !"

    // print to command window
    System.out.println('Result : ' + resultString)
    System.out.println('End : Clean Buildings')
    System.out.println('Duration : ' + TimeCategory.minus(new Date(), start))

    // print to WPS Builder
    return resultString


}
