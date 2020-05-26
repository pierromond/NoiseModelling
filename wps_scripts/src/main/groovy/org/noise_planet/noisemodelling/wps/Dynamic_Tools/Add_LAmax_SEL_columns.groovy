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
 * @Author Pierre Aumond, Univ Gustave Eiffel
 * @Author Arnaud Can, Univ Gustave Eiffel
 */

package org.noise_planet.noisemodelling.wps.Others_Tools

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.utilities.JDBCUtilities

import java.sql.Connection

title = 'Create LAmax and SEL columns'
description = 'Create a table with LAmax and SEL metrics for each receiver from a table which contain a time frame and octave band values from 63 Hz to 8000 Hz. </br> The columns of the table should be named TIME, HZ63, HZ125,..., HZ8000 with an HZ prefix that can be changed.'

inputs = [prefix   : [name       : 'Prefix of the octave bands columns', title: 'Prefix of the frequency bands column',
                      description: 'Prefix of the columns containing the octave bands. (STRING) </br> For example : HZ', type: String.class],
          tableName: [name: 'Name of the table', description: 'Name of the table to which a primary key will be added.', title: 'Name of the table', type: String.class]]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'This type of result does not allow the blocks to be linked together.', type: String.class]]


static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}

def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = null

    // print to command window
    System.out.println('Start : Compute LAmax and SEL columns')
    def start = new Date()
    // -------------------
    // Get inputs
    // -------------------

    // Get name of the prefix
    String prefix = input['prefix'] as String
    // do it case-insensitive
    prefix = prefix.toUpperCase()

    // Get name of the table
    String table = input["tableName"] as String
    // do it case-insensitive
    table = table.toUpperCase()


    List<String> fields = JDBCUtilities.getFieldNames(connection.getMetaData(), table)
    if (!fields.contains("" + prefix + "63")) {
        resultString = "This table does not contain column with this prefix : " + prefix + ""
        return resultString
    }
    if (!fields.contains("TIME")) {
    	resultString = "This table does not contain a time column"
    	return resultString
    }

    // Open connection
    Sql sql = new Sql(connection)
    sql.execute("drop table if exists TEMP;")
    sql.execute("create table TEMP (IDRECEIVER integer, LAMAX double precision, SEL double precision);")
    List<Integer> idReceivers = new ArrayList<Integer>()
    sql.eachRow("SELECT DISTINCT IDRECEIVER FROM " + table + ";") { row ->
    	idReceivers.add(row[0])
    }
    System.out.println(idReceivers.size())
    for (int i = 0; i < idReceivers.size(); i++) {
    	sql.execute("INSERT INTO TEMP VALUES (" + idReceivers[i].toString() + ", SELECT MAX(LEQA) FROM " + table + " WHERE IDRECEIVER = " + idReceivers[i].toString() + ", SELECT 10*log10(SUM(power(10,LEQA/10))) FROM " + table + " WHERE IDRECEIVER = " + idReceivers[i].toString() + ");")
    }
    
    sql.execute("drop table if exists NOISE_METRICS;")
    sql.execute("create table NOISE_METRICS as select a.IDRECEIVER, b.THE_GEOM, a.LAMAX, a.SEL FROM TEMP a LEFT JOIN RECEIVERS b ON a.IDRECEIVER = b.PK;")

    resultString = "The columns LAMAX and SEL have been computed."

    // print to command window
    System.out.println('Result : ' + resultString)
    System.out.println('End : Compute LAmax and SEL columns')
    System.out.println('Duration : ' + TimeCategory.minus(new Date(), start))

    // print to WPS Builder
    return resultString

}

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
