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
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */


package org.noise_planet.noisemodelling.scripts.Database_Manager

import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.Locale

title = 'Display the list of tables (and their attributes).'
description = '&#10145;&#65039; Displays the list of tables that are in the database. </br> ' +
              '<hr>' +
              'Optionally it is also possible to display their attributes ("showColumns" parameter). </br> </br>' +
              '&#128161; To visualize the content of (a part of) a table, you can use "Table Visualization Data" script.'

inputs = [
        showColumns: [
                name       : 'Display columns of the tables',
                title      : 'Display columns of the tables',
                description: 'Do you want to display also the column of the tables ? </br></br>' +
                             '&#128161; Note : A small yellow key symbol (&#128273;) will appear if the column as a Primary Key constraint.',
                type       : Boolean.class,
                min        : 0, max: 1
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
    logger.info('Start : Display database')
    logger.info("inputs {}", input) // log inputs of the run

    Boolean showColumnName = false
    
    if(input['showColumns']) {
        showColumnName = input['showColumns'].toBoolean()
    }

    // list of the system tables (H2GIS and PostGIS)
    List<String> ignorelst = [
        "SPATIAL_REF_SYS", "GEOMETRY_COLUMNS", "GEOGRAPHY_COLUMNS",
        "SPATIAL_REF_SYS_PKEY", "GEOMETRY_DUMP", "VALID_DETAIL",
        "RASTER_COLUMNS", "RASTER_OVERVIEWS"
    ]

    // Build the result string with every tables
    StringBuilder sb = new StringBuilder()

    // Get database type to determine schema name
    DBTypes dbType = DatabaseHelper.getDBType(connection)
    boolean isPostgreSQL = dbType == DBTypes.POSTGRESQL
    String schemaName
    
    if (isPostgreSQL) {
        // Get the current schema from search_path
        def stmt = connection.createStatement()
        def rs = stmt.executeQuery("SELECT current_schema()")
        schemaName = rs.next() ? rs.getString(1) : "public"
        rs.close()
        stmt.close()
    } else {
        schemaName = "PUBLIC"
    }

    // Get every table names (TABLE type only, not VIEWs or INDEXes)
    List<String> tables
    
    if (isPostgreSQL) {
        // For PostGIS, query pg_tables using the current schema
        def stmt = connection.createStatement()
        def rs = stmt.executeQuery("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = '${schemaName}'
            AND tablename NOT IN ('spatial_ref_sys', 'geometry_columns', 'geography_columns', 'raster_columns', 'raster_overviews')
        """)
        tables = []
        while (rs.next()) {
            tables.add(rs.getString("tablename"))
        }
        rs.close()
        stmt.close()
    } else {
        tables = JDBCUtilities.getTableNames(connection, null, schemaName, "%", ["TABLE"] as String[])
    }
    
    // Sort tables case-insensitively for deterministic output
    tables.sort { it.toUpperCase() }

    // Loop over the tables
    tables.each { t ->
        TableLocation tab = TableLocation.parse(t, dbType)
        String tableName = tab.getTable()
        // Check if this is a system table to ignore (compare in uppercase for consistency)
        if (!ignorelst.contains(tableName.toUpperCase())) {
            String displayName = isPostgreSQL ? tableName.toLowerCase(Locale.ROOT) : tableName.toUpperCase(Locale.ROOT)
            sb.append(displayName)
            sb.append("</br>")
            if (showColumnName) {
                List<String> fields = JDBCUtilities.getColumnNames(connection, t)
                Integer keyColumnIndex = DatabaseHelper.getPrimaryKeyIndex(connection, tableName)
                int columnIndex = 1;
                fields.each {
                    f ->
                        if (columnIndex == keyColumnIndex) {
                            sb.append(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%s&nbsp;&#128273;</br>", f))
                        } else {
                            sb.append(String.format("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%s</br>", f))
                        }
                        columnIndex++
                }
            }
            sb.append("</br>")
        }
    }

    // print to command window
    logger.info('End : Display database')

    // print to WPS Builder
    return sb.toString()
}