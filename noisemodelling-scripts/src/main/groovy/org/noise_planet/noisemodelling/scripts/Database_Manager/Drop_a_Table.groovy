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

package org.noise_planet.noisemodelling.scripts.Database_Manager

import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.Statement

title = 'Remove a table from the database.'
description = '&#10145;&#65039; Remove a table from the database. </br> </br>' +
              '&#x1F6A8; Use with caution'

inputs = [
        tableToDrop: [
                name       : 'Name of the table to drop',
                title      : 'Name of the table to drop',
                description: 'Name of the table to drop',
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
    logger.info('Start : Drop a table')
    logger.info("inputs {}", input) // log inputs of the run


    // Get name of the table to drop (use as-is - databases handle case naturally)
    String tableToDrop = input['tableToDrop'] as String

    // list of the system tables
    List<String> ignorelst = ["SPATIAL_REF_SYS", "GEOMETRY_COLUMNS"]

    // flag to get out of the loop
    int flag = 0

    // Normalize table name for comparison and output message
    String tableToDropNormalized = DatabaseHelper.normalizeTableName(connection, tableToDrop)

    // Get schema name based on database type
    DBTypes dbType = DatabaseHelper.getDBType(connection)
    String schemaName
    if (dbType == DBTypes.POSTGRESQL) {
        // Get current schema for PostgreSQL
        def stmt = connection.createStatement()
        def rs = stmt.executeQuery("SELECT current_schema()")
        schemaName = rs.next() ? rs.getString(1) : "public"
        rs.close()
        stmt.close()
    } else {
        schemaName = "PUBLIC"
    }

    // Get every table names
    List<String> tables = JDBCUtilities.getTableNames(connection, null, schemaName, "%", null)
    // Loop over the tables
    tables.each { t ->
        TableLocation tab = TableLocation.parse(t)
        if (!ignorelst.contains(tab.getTable())) {
            // If name of the actual table is the same than the name of the table to drop (case-insensitive comparison)
            if (tab.getTable() == tableToDropNormalized) {
                // Create a connection statement to interact with the database in SQL
                Statement stmt = connection.createStatement()
                // Drop the table
                String dropTable = "Drop table if exists " + tableToDrop
                stmt.execute(dropTable)
                resultString = "The table " + tableToDropNormalized + " was dropped !"
                flag = 1
            }
        }
    }

    if (flag == 0) {
        logger.warn("The table " + tableToDrop + " was not found")
        resultString = "The table " + tableToDrop + " was not found"
    }

    // print to command window
    logger.info('End : Drop a table')

    // print to WPS Builder
    return resultString
}
