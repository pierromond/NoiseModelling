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

import groovy.sql.Sql
import org.h2gis.utilities.JDBCUtilities
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTWriter
import org.locationtech.jts.io.WKBReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection


title = 'Display first rows of a table.'
description = '&#10145;&#65039; Display the content of a table. </br>' +
              '<hr>' +
              'Using "linesNumber" parameter, you can choose the number of lines to display </br> </br>' +
              '&#x1F6A8; Be careful, this treatment can be very long if the table is large.'

inputs = [
        linesNumber: [
                name       : 'Number of rows',
                title      : 'Number of rows',
                description: 'Number of rows you want to display (INTEGER) </br> </br>' +
                             '&#128736; Default value: <b>10 </b> ',
                min        : 0, max: 1,
                type       : Integer.class
        ],
        tableName  : [
                name       : 'Name of the table',
                title      : 'Name of the table',
                description: 'Name of the table you want to display',
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

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Display first rows of a table')
    logger.info("inputs {}", input) // log inputs of the run

    // Get the number of rows the user want to display
    int linesNumber = 10
    if (input['linesNumber']) {
        linesNumber = input['linesNumber'] as Integer
    }

    // Get name of the table (use as-is - databases handle case naturally)
    String tableName = input["tableName"] as String

    // Create a connection statement to interact with the database in SQL
    Sql sql = new Sql(connection)

    List output = sql.rows(String.format("select * from %s LIMIT %s", tableName, linesNumber.toString()))

    logger.info('End : Display first rows of a table')


    // print to WPS Builder
    return mapToTable(output, sql, tableName, connection)
}



/**
 * Convert a list to HTML table
 * @param list
 * @return
 */
static String mapToTable(List<Map> list, Sql sql, String tableName, Connection connection) {

    StringBuilder output = new StringBuilder()

    Map first = list.first()

    output.append("The total number of rows is " + sql.firstRow('SELECT COUNT(*) FROM ' + tableName)[0])

    //get SRID of the table
    String normalizedTableName = DatabaseHelper.normalizeTableName(connection, tableName)
    int srid = DatabaseHelper.getTableSRID(connection, normalizedTableName)

    if (srid > 0) {
        output.append("</br>")
        output.append("The srid of the table is " + srid)
    } else {
        output.append("</br>")
        output.append("This table doesn't have any srid")
    }

    //get primary key index of the table
    int pkIndex = DatabaseHelper.getPrimaryKeyIndex(connection, tableName)

    if (pkIndex > 0) {
        output.append("</br>")
        output.append("The table has the following primary key : " + JDBCUtilities.getColumnName(connection, tableName, pkIndex))
    } else {
        output.append("</br>")
        output.append("This table does not have primary key.")
    }


    output.append("</br> </br> ")
    output.append("<table  border=' 1px solid black'><thead><tr>")

    first.each { key, val ->
        output.append("<th>${key}</th>")
    }

    output.append("</tr></thead><tbody>")
    WKTWriter wktWriter = new WKTWriter(3)
    WKBReader wkbReader = new WKBReader()
    boolean isPostgreSQL = DatabaseHelper.isPostgreSQL(connection)

    list.each { map ->
        if (!map.isEmpty()) {
            def values = map.values()
            output.append("<tr>")

            values.each { value ->
                def cellValue = value
                if (cellValue instanceof Geometry) {
                    cellValue = wktWriter.write(cellValue)
                } else if (isPostgreSQL && value instanceof org.postgresql.util.PGobject) {
                    org.postgresql.util.PGobject pgObject = (org.postgresql.util.PGobject) value
                    String pgValue = pgObject.value
                    if (pgValue) {
                        try {
                            Geometry geom = wkbReader.read(hexStringToByteArray(pgValue))
                            cellValue = wktWriter.write(geom)
                        } catch (Exception ignore) {
                            cellValue = pgValue
                        }
                    } else {
                        cellValue = pgValue
                    }
                }
                output.append "<td><div style='width: 150px;'>${cellValue}</div></td>"
            }

            output.append("</tr>")
        }
    }
    output.append("</tbody></table>")

    return output.toString()
}

static byte[] hexStringToByteArray(String hex) {
    int len = hex.length()
    byte[] data = new byte[len / 2]
    for (int i = 0; i < len; i += 2) {
        int high = Character.digit(hex.charAt(i), 16)
        int low = Character.digit(hex.charAt(i + 1), 16)
        data[i / 2] = (byte) ((high << 4) + low)
    }
    return data
}