package org.noise_planet.noisemodelling.utils

import groovy.sql.Sql
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.slf4j.Logger

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet

class IndexUtilities {
    static void ensureIndex(Connection connection, Sql sql, Logger logger, String tableName, String columnName) {
        logger.info("searching index on {}.{} ...", tableName, columnName)
        DatabaseMetaData dbMeta = connection.getMetaData()
        ResultSet rs = dbMeta.getIndexInfo(null, null, tableName, false, false)
        boolean indexFound = false
        try {
            while (rs.next()) {
                String indexedColumn = rs.getString("COLUMN_NAME")
                String pos = rs.getString("ORDINAL_POSITION")
                if (indexedColumn != null && indexedColumn.equalsIgnoreCase(columnName) && pos == "1") {
                    indexFound = true
                    logger.info("index on {}.{} found", tableName, columnName)
                    break
                }
            }
        } finally {
            rs.close()
        }
        if (!indexFound) {
            logger.info("index on {}.{}, NOT found, creating one...", tableName, columnName)
            sql.execute("CREATE INDEX ON " + tableName + " (" + columnName + ")")
        }
    }

    static void ensureSpatialIndex(Connection connection, Logger logger, String tableName) {
        String geomCol = GeometryTableUtilities.getFirstGeometryColumnNameAndIndex(connection, tableName).first()
        logger.info("searching spatial index on {}.{} ...", tableName, geomCol)
        if (!JDBCUtilities.isSpatialIndexed(connection, tableName, geomCol)) {
            logger.info("spatial index on {}.{}, NOT found, creating one...", tableName, geomCol)
            JDBCUtilities.createSpatialIndex(connection, tableName, geomCol)
        } else {
            logger.info("spatial index on {}.{} found", tableName, geomCol)
        }
    }
}