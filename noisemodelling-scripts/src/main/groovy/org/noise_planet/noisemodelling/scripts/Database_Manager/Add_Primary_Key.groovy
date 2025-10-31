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
 */

package org.noise_planet.noisemodelling.scripts.Database_Manager

import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

title = 'Add primary key column or constraint'
description = '&#10145;&#65039; Add a Primary Key (&#128273;) column or add a Primary Key constraint to a column of a table. </br> ' +
              '<hr>' +
              'It is necessary to add a Primary Key on one of the columns for the source and receiver tables before doing a calculation. </br> </br>' +
              '&#128161; If the table already has a Primary Key, it will remove the constraint before the operation.'

inputs = [
        pkName: [
                name: 'Name of the column',
                title: 'Name of the column',
                description: 'Name of the column to be added, or for which the main key constraint will be added. </br> </br>'+
                '&#128161; Primary keys must contain <b>UNIQUE</b> values, and cannot contain <b>NULL</b> values',
                type: String.class
        ],
        tableName : [
                name: 'Name of the table',
                title: 'Name of the table',
                description: 'Name of the table on which a primary key will be added',
                type: String.class
        ]
]

outputs = [
        result: [
                name: 'Result output string',
                title: 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type: String.class
        ]
]

def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = ""

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info("inputs {}", input) // log inputs of the run

    // print to command window
    logger.info('Start : Add primary key column or constraint')

    // Get name of the table (use as-is - databases handle case naturally)
    String table = input["tableName"] as String

    // Get name of the pk field (use as-is - databases handle case naturally)
    String pkName = input['pkName'] as String

    // Create a connection statement to interact with the database in SQL
    Statement stmt = connection.createStatement()

    // Normalize for H2GIS utility method calls
    String table_for_utils = DatabaseHelper.normalizeTableNameForUtilities(connection, table)
    
    // Check if PostgreSQL for database-specific syntax
    boolean isPostgreSQL = DatabaseHelper.isPostgreSQL(connection)
    
    // get the index of the primary key column (if exists > 0)
    int pkIndex
    if (isPostgreSQL) {
        // For PostgreSQL, query information_schema directly since JDBCUtilities has issues with case sensitivity
        ResultSet rsCheck = stmt.executeQuery("""
            SELECT a.attnum
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '${table}'::regclass
            AND i.indisprimary
            LIMIT 1
        """)
        pkIndex = rsCheck.next() ? rsCheck.getInt(1) : 0
        rsCheck.close()
    } else {
        // For H2GIS, use the utility method
        pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse(table_for_utils))
    }

    // get the index of the column given by the user (if exists > 0)
    ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)
    int pkUserIndex = JDBCUtilities.getFieldIndex(rs.getMetaData(), pkName)

    // Normalize table name for output message (consistent with old behavior)
    String tableForMessage = DatabaseHelper.normalizeTableName(connection, table)
    
    if (pkIndex > 0) {
        resultString = String.format("Warning : Source table %s did already contain a primary key. The constraint has been removed. </br>", tableForMessage)
        logger.warn(String.format("Warning : Source table %s did already contain a primary key. The constraint has been removed.", tableForMessage))
        
        // Database-specific DROP PRIMARY KEY syntax
        if (isPostgreSQL) {
            // For PostgreSQL, find and drop the constraint
            ResultSet rsConstraint = stmt.executeQuery("SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = LOWER('${table}') AND constraint_type = 'PRIMARY KEY'")
            if (rsConstraint.next()) {
                String constraintName = rsConstraint.getString(1)
                rsConstraint.close()
                stmt.execute("ALTER TABLE " + table + " DROP CONSTRAINT \"${constraintName}\"")
            } else {
                rsConstraint.close()
            }
        } else {
            stmt.execute("ALTER TABLE " + table + " DROP PRIMARY KEY;")
        }
    }

    if (pkUserIndex > 0) {
        // Database-specific ALTER COLUMN syntax
        if (isPostgreSQL) {
            stmt.execute("ALTER TABLE " + table + " ALTER COLUMN " + pkName + " TYPE INTEGER;")
            stmt.execute("ALTER TABLE " + table + " ALTER COLUMN " + pkName + " SET NOT NULL;")
        } else {
            stmt.execute("ALTER TABLE " + table + " ALTER COLUMN " + pkName + " INT NOT NULL;")
        }
        stmt.execute("ALTER TABLE " + table + " ADD PRIMARY KEY (" + pkName + ");  ")
        resultString = resultString + String.format(tableForMessage + " has a new primary key constraint on " + pkName + ".")
    } else {
        // Database-specific syntax for adding auto-increment primary key column
        if (isPostgreSQL) {
            stmt.execute("ALTER TABLE " + table + " ADD " + pkName + " SERIAL PRIMARY KEY;")
        } else {
            stmt.execute("ALTER TABLE " + table + " ADD " + pkName + " INT AUTO_INCREMENT PRIMARY KEY;")
        }
        resultString = resultString + String.format(tableForMessage + " has a new primary key column which is called " + pkName + ".")
    }

    // print to command window
    logger.info('Result : ' + resultString)
    logger.info('End : Add primary key column or constraint')

    // print to WPS Builder
    return resultString

}