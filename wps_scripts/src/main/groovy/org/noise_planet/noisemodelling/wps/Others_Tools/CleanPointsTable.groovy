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

import java.sql.Connection

title = 'Clean Buildings Table'
description = 'Clean Buildings Table. ' +
        ' <br>Avoid overlapping areas.' 

inputs = [tableBuilding     : [name       : 'Buildings table name', title: 'Buildings table name',
                               description: '<b>Name of the Buildings table.</b>  </br>  ' +
                                       '<br>  The table shall contain : </br>' +
                                       '- <b> THE_GEOM </b> : the 2D geometry of the building (POLYGON or MULTIPOLYGON). </br>' +
                                       '- <b> HEIGHT </b> : the height of the building (FLOAT)',
                               type       : String.class],
          points_table_name    : [name: 'Points table name', title: 'Points table name',
                               description: '<b>Name of the Receivers table.</b></br>  ' +
                                       '</br>  The table shall contain : </br> ' +
                                       '- <b> PK </b> : an identifier. It shall be a primary key (INTEGER, PRIMARY KEY). </br> ' +
                                       '- <b> THE_GEOM </b> : the 3D geometry of the sources (POINT, MULTIPOINT).</br> ' +
                                       '</br> </br> <b> This table can be generated from the WPS Blocks in the "Receivers" folder. </b>',
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


    String points_table_name = input['points_table_name']
    // do it case-insensitive
    points_table_name = points_table_name.toUpperCase()

    // -------------------------
    // Initialize some variables
    // -------------------------

   
   // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    System.out.println("Delete receivers inside buildings")

    sql.execute("Create spatial index on " + building_table_name + "(the_geom);")
    sql.execute("delete from " + points_table_name + " g where exists (select 1 from " + building_table_name + " b where ST_Z(g.the_geom) < b.HEIGHT and g.the_geom && b.the_geom and ST_INTERSECTS(g.the_geom, b.the_geom) and ST_distance(b.the_geom, g.the_geom) < 1 limit 1);")

    resultString = resultString + "Calculation Done !"

    // print to command window
    System.out.println('Result : ' + resultString)
    System.out.println('End : Clean Buildings')
    System.out.println('Duration : ' + TimeCategory.minus(new Date(), start))

    // print to WPS Builder
    return resultString


}
