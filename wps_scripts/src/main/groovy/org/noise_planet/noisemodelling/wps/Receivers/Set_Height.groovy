/**
 * @Author Aumond Pierre, Université Gustave Eiffel
 */

package org.noise_planet.noisemodelling.wps.Receivers

import geoserver.GeoServer
import geoserver.catalog.Store
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader

import java.sql.*
import groovy.sql.Sql

title = 'Set_Height'
description = 'Set new height of a set of receivers.'

inputs = [receiverstablename: [name: 'receiverstablename', description: 'Do not write the name of a table that contains a space. (default : RECEIVERS)', title: 'Name of receivers table', min: 0, max: 1, type: String.class],
          height            : [name: 'height', title: 'height', description: 'Height of receivers in meters',type: Double.class]]

outputs = [tableNameCreated: [name: 'tableNameCreated', title: 'tableNameCreated', type: String.class]]

static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}


def run(input) {

    // Get name of the database
    String dbName = ""
    if (input['databaseName']) {
        dbName = input['databaseName'] as String
    }

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable { Connection connection -> exec(connection, input)
    }
}


def exec(connection, input) {

    String receivers_table_name = "RECEIVERS"
    if (input['receiverstablename']) {
        receivers_table_name = input['receiverstablename']
    }
    receivers_table_name = receivers_table_name.toUpperCase()

    Double h = input['height']
    
    Sql sql = new Sql(connection)
    
    sql.execute("UPDATE RECEIVERS SET THE_GEOM = ST_UPDATEZ(ST_Force3D(THE_GEOM), "+h+");")

    return [tableNameCreated: "Process done. Table of receivers " + receivers_table_name + " has now a new height !"]
}

