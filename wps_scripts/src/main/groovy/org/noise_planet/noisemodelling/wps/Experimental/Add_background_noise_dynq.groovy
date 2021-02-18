package org.noise_planet.noisemodelling.wps.Experimental

/*
 * @Author Arnaud Can
 */

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import org.geotools.jdbc.JDBCDataStore

import java.sql.Connection

title = 'Add background Noise Dynamic'
description = 'Adds a background noise value to each column in frequency and fills timesteps with no value '

inputs = [databaseName      : [name: 'Name of the database', title: 'Name of the database', description: 'Name of the database. (default : h2gisdb)', min: 0, max: 1, type: String.class],
          inputlevels  : [name: 'Sound levels per frequency bands', title: 'Sound levels per frequency bands',description: 'columns values per freq', type: String.class],
          receiversTableName: [name: 'Receivers table name', title: 'Receivers table name', type: String.class], 
          bknoisevalue    : [name: 'Background noise value per frequency band', title: 'Background noise value per frequency band', description: 'Background Noise value per frequency (filled as "[x63,x125,x250,x500,x1k,x2k,x4k,x8k]")', type: String.class]]

outputs = [result: [name: 'result', title: 'Result', type: String.class]]


def simplisticParse( String input, Class requiredType ) {
  input.dropWhile { it != '[' }
       .drop( 1 )
       .takeWhile { it != ']' }
       .split( ',' )*.asType( requiredType )
}

static Connection openGeoserverDataStoreConnection(String dbName) {
    if(dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore)store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}


def run(input) {

    // -------------------
    // Get inputs
    // -------------------

    String input_levels = "InputLevels"
    if (input['inputlevels']) {
        input_levels = input['inputlevels']
    }
    input_levels = input_levels.toUpperCase()

    String background_noise_levels = "bknoisevalue"
    if (input['bknoisevalue']) {
        background_noise_levels = input['bknoisevalue']
    }

    String receivers_table_name = "RECEIVERS"
    if (input['receiversTableName']) {
        receivers_table_name = input['receiversTableName']
    }
    receivers_table_name = receivers_table_name.toUpperCase()

    // Get name of the database
    String dbName = "h2gisdb"
    if (input['databaseName']) {
        dbName = input['databaseName'] as String
    }


    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable { Connection connection ->
        Sql sql = new Sql(connection)


    Double[] i = simplisticParse( background_noise_levels, Double)
    System.println(i)
 
    sql.execute("DROP TABLE IF EXISTS Temps ;")
    sql.execute("CREATE TABLE Temps (TIME int primary key) AS SELECT DISTINCT TIME FROM "+input_levels+" ;")
    sql.execute("DROP TABLE IF EXISTS Positions_rec ;")
    sql.execute("CREATE TABLE Positions_rec (Idrec int primary key, the_geom geometry) AS SELECT DISTINCT PK, the_geom FROM "+receivers_table_name+" ;")
    sql.execute("DROP TABLE IF EXISTS BkNoise;")
    sql.execute("CREATE TABLE BkNoise (Idrec int, Time int, the_geom geometry, primary key (Idrec,Time)) as select p.Idrec,tps.TIME, p.the_geom from Positions_rec p, Temps tps;")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ63 float as "+i[0]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ125 float as "+i[1]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ250 float as "+i[2]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ500 float as "+i[3]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ1000 float as "+i[4]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ2000 float as "+i[5]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ4000 float as "+i[6]+";")
    sql.execute("ALTER TABLE BkNoise ADD Column HZ8000 float as "+i[7]+";")
    sql.execute("DROP TABLE IF EXISTS L_Cars_with_bknoise ;")
    sql.execute("CREATE TABLE  L_Cars_with_bknoise (Idrec int, Time int, the_geom geometry, HZ63 float, HZ125 float, HZ250 float, HZ500 float, HZ1000 float, HZ2000 float, HZ4000 float, HZ8000 float) as select bkn.Idrec, bkn.time, bkn.the_geom, \n"+
    "10*log10(power(10,bkn.HZ63/10)+power(10,(ISNULL(Lcars.HZ63,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ125/10)+power(10,(ISNULL(Lcars.HZ125,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ250/10)+power(10,(ISNULL(Lcars.HZ250,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ500/10)+power(10,(ISNULL(Lcars.HZ500,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ1000/10)+power(10,(ISNULL(Lcars.HZ1000,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ2000/10)+power(10,(ISNULL(Lcars.HZ2000,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ4000/10)+power(10,(ISNULL(Lcars.HZ4000,0))/10)), \n"+
    "10*log10(power(10,bkn.HZ8000/10)+power(10,(ISNULL(Lcars.HZ8000,0))/10)) from BkNoise bkn left join "+input_levels+" Lcars on bkn.Idrec=Lcars.IDRECEIVER and bkn.time=Lcars.time;")


    }


    // Process Done
    return [result: "background noise added !"]

}

/**
 *
 */
