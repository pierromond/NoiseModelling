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
 * @Author Arnaud Can, Université Gustave Eiffel
 */

package org.noise_planet.noisemodelling.scripts.Acoustic_Tools

import groovy.sql.Sql
import org.h2gis.utilities.JDBCUtilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection

title = 'Add Leq and LAeq columns'
description = '&#10145;&#65039; Add the columns <b>Leq</b> and <b>LAeq</b> to a table with octave band values from 63 Hz to 8000 Hz.'+
              '<hr>' +
              'The columns of the table should be named HZ63, HZ125,..., HZ8000 with an HZ prefix that can be changed.'

inputs = [
        prefix   : [
                name       : 'Prefix of the frequency bands column',
                title      : 'Prefix of the frequency bands column',
                description: 'Prefix of the columns containing the octave bands. (STRING)</br> </br>'+
                             'For example: HZ',
                type       : String.class
        ],
        tableName: [
                title      : 'Name of the table',
                name       : 'Name of the table',
                description: 'Name of the table on which <b>Leq</b> and <b>LAeq</b> columns will be added.',
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

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Add Leq and LAeq column')
    logger.info("inputs {}", input) // log inputs of the run

    // Open connection
    Sql sql = new Sql(connection)

    // -------------------
    // Get inputs
    // -------------------

    // Détecter le type de base de données (pour gérer la casse)
    String dbProductName = connection.getMetaData().getDatabaseProductName()
    boolean isPostgreSQL = dbProductName.toLowerCase().contains("postgresql")

    // Get name of the prefix (use as-is from user)
    String prefix = input['prefix'] as String

    // Get name of the table (use as-is from user - databases handle case naturally)
    String table = input["tableName"] as String

    List<String> fields = JDBCUtilities.getColumnNames(connection, table)
    
    // Check for column existence (case-insensitive for PostgreSQL compatibility)
    String checkColumn = prefix + "63"
    boolean hasColumn = fields.any { it.equalsIgnoreCase(checkColumn) }
    if (!hasColumn) {
        resultString = "This table does not contain column with this suffix : " + prefix + ""
        return resultString
    }

    // Pour PostGIS, on utilise une approche en deux étapes: ADD COLUMN + UPDATE
    // Pour H2GIS, on peut utiliser la colonne calculée
    if (isPostgreSQL) {
        sql.execute("ALTER TABLE " + table + " ADD COLUMN LEQA float")
        sql.execute("ALTER TABLE " + table + " ADD COLUMN LEQ float")
        
        sql.execute("UPDATE " + table + " SET LEQA = 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))")
        sql.execute("UPDATE " + table + " SET LEQ = 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))")
    } else {
        sql.execute("ALTER TABLE " + table + " ADD COLUMN LEQA float as 10*log10((power(10,(" + prefix + "63-26.2)/10)+power(10,(" + prefix + "125-16.1)/10)+power(10,(" + prefix + "250-8.6)/10)+power(10,(" + prefix + "500-3.2)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000+1.2)/10)+power(10,(" + prefix + "4000+1)/10)+power(10,(" + prefix + "8000-1.1)/10)))")
        sql.execute("ALTER TABLE " + table + " ADD COLUMN LEQ float as 10*log10((power(10,(" + prefix + "63)/10)+power(10,(" + prefix + "125)/10)+power(10,(" + prefix + "250)/10)+power(10,(" + prefix + "500)/10)+power(10,(" + prefix + "1000)/10)+power(10,(" + prefix + "2000)/10)+power(10,(" + prefix + "4000)/10)+power(10,(" + prefix + "8000)/10)))")
    }

    resultString = "The columns LEQA and LEQ have been added to the table: " + table + "."

    // print to command window
    logger.info('End : Add Leq and LAeq column')

    // print to WPS Builder
    return resultString

}
