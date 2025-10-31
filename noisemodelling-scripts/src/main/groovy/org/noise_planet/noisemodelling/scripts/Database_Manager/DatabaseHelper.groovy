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
 * Database Helper - Utilities for cross-database compatibility (H2GIS / PostGIS)
 * 
 * <h2>Overview</h2>
 * This helper class provides a unified API for database operations that work across both H2GIS 
 * (in-memory embedded database) and PostgreSQL with PostGIS extension. It abstracts away the 
 * differences in SQL syntax, metadata handling, and geometry operations between these databases.
 * 
 * <h2>Key Design Principles</h2>
 * <ul>
 *   <li><b>Database Detection</b>: Use {@link #isPostgreSQL(Connection)} to detect database type</li>
 *   <li><b>Naming Conventions</b>: H2GIS uses UPPERCASE, PostgreSQL uses lowercase for identifiers</li>
 *   <li><b>Geometry Handling</b>: Different JDBC types (Geometry vs PGobject) require special handling</li>
 *   <li><b>SRID Management</b>: PostgreSQL requires explicit SRID metadata in geometry_columns table</li>
 *   <li><b>Spatial Indexing</b>: H2GIS uses R-tree, PostgreSQL uses GiST</li>
 * </ul>
 * 
 * <h2>Common Usage Patterns</h2>
 * 
 * <h3>1. Table Name Normalization</h3>
 * <pre>
 * // Always normalize table names before using in SQL
 * String normalizedTable = DatabaseHelper.normalizeTableName(connection, "MyTable")
 * // H2GIS: "MYTABLE", PostgreSQL: "mytable"
 * </pre>
 * 
 * <h3>2. SRID Detection and Enforcement</h3>
 * <pre>
 * // Detect SRID from existing geometries
 * int srid = DatabaseHelper.getTableSRID(connection, "buildings", "the_geom")
 * 
 * // Ensure SRID is set correctly (especially after shapefile imports)
 * DatabaseHelper.ensureSRID(connection, "buildings", "the_geom", 2154)
 * </pre>
 * 
 * <h3>3. Spatial Index Creation</h3>
 * <pre>
 * // Creates appropriate index type for each database
 * DatabaseHelper.createSpatialIndex(connection, "buildings", "the_geom")
 * // H2GIS: CREATE SPATIAL INDEX ... ON ...
 * // PostgreSQL: CREATE INDEX ... USING GIST (...)
 * </pre>
 * 
 * <h3>4. Geometry Parameter Handling</h3>
 * <pre>
 * Geometry fence = ... // JTS Geometry with SRID
 * def params = DatabaseHelper.prepareGeometryParameter(connection, fence, "fence")
 * sql.execute("SELECT * FROM buildings WHERE the_geom && ${params.expression}", params.parameters)
 * </pre>
 * 
 * <h3>5. Table Envelope Extraction</h3>
 * <pre>
 * // Works on both databases despite different internal implementations
 * Geometry bbox = DatabaseHelper.getTableEnvelope(connection, "buildings", "the_geom")
 * Envelope env = bbox.getEnvelopeInternal()
 * </pre>
 * 
 * <h2>PostgreSQL-Specific Considerations</h2>
 * <ul>
 *   <li>Shapefile imports may not set SRID metadata properly - always call {@link #ensureSRID}</li>
 *   <li>JDBC returns {@code PGobject} instead of JTS {@code Geometry} - use WKBReader</li>
 *   <li>Geometry column definitions require explicit type and SRID: {@code geometry(Point, 2154)}</li>
 *   <li>PRIMARY KEY constraints have names that must be queried from pg_constraint</li>
 *   <li>Function names differ: {@code RANDOM()} vs {@code RAND()}, {@code SERIAL} vs {@code AUTO_INCREMENT}</li>
 * </ul>
 * 
 * @see org.h2gis.utilities.GeometryTableUtilities
 * @see org.locationtech.jts.geom.Geometry
 */

package org.noise_planet.noisemodelling.wps.Database_Manager

import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.dbtypes.DBTypes
import org.h2gis.functions.io.shp.SHPRead
import org.h2.value.ValueBoolean
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.io.WKBReader
import org.postgresql.util.PGobject

import java.math.BigDecimal

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Helper class for database operations that work across H2GIS and PostGIS.
 * <p>
 * This class provides abstraction over database-specific differences in:
 * <ul>
 *   <li>Identifier casing (H2GIS uppercase vs PostgreSQL lowercase)</li>
 *   <li>Geometry metadata management (geometry_columns table)</li>
 *   <li>SRID handling (explicit vs implicit)</li>
 *   <li>Spatial index syntax (R-tree vs GiST)</li>
 *   <li>JDBC geometry types (Geometry vs PGobject)</li>
 * </ul>
 */
class DatabaseHelper {

    /**
     * Container for geometry parameter metadata when using named parameters in Groovy SQL.
     * <p>
     * Example usage:
     * <pre>
     * Geometry fence = ...
     * GeometryParameter param = prepareGeometryParameter(connection, fence, "fence")
     * sql.execute("SELECT * FROM t WHERE geom && ${param.expression}", param.parameters)
     * </pre>
     */
    static class GeometryParameter {
        /** SQL expression to use in query (e.g., "ST_SetSRID(ST_GeomFromText(:fenceWkt), :fenceSrid)") */
        String expression
        
        /** Parameter map to pass to Groovy SQL (e.g., [fenceWkt: "POLYGON(...)", fenceSrid: 2154]) */
        Map<String, Object> parameters
    }

    /**
     * Detect database type from connection
     * @param connection Database connection
     * @return DBTypes enum (POSTGRESQL or H2GIS)
     */
    static DBTypes getDBType(Connection connection) {
        String dbProductName = connection.getMetaData().getDatabaseProductName()
        return dbProductName.toLowerCase().contains("postgresql") ? DBTypes.POSTGRESQL : DBTypes.H2GIS
    }

    /**
     * Check if connection is PostgreSQL
     * @param connection Database connection
     * @return true if PostgreSQL, false otherwise
     */
    static boolean isPostgreSQL(Connection connection) {
        return getDBType(connection) == DBTypes.POSTGRESQL
    }

    /**
     * Returns a geometry column definition that works on both databases.
     * When SRID is provided (>0) the column is declared with explicit SRID metadata.
     * @param connection Database connection
     * @param columnName Geometry column name
     * @param geometryType Geometry type (e.g. "Point", "PointZ", "Polygon")
     * @param srid Spatial reference identifier, optional (<=0 to omit)
     * @return Column definition string (e.g. "the_geom geometry(PointZ, 2154)")
     */
    static String geometryColumnDefinition(Connection connection, String columnName, String geometryType = null, int srid = 0) {
        StringBuilder definition = new StringBuilder()
        definition.append(columnName).append(" ")

        List<String> typeParts = []
        if (geometryType) {
            typeParts.add(geometryType)
        }
        if (srid > 0) {
            typeParts.add(String.valueOf(srid))
        }

        if (typeParts.isEmpty()) {
            definition.append("geometry")
        } else {
            definition.append("geometry(").append(typeParts.join(', ')).append(")")
        }

        return definition.toString()
    }

    /**
     * Normalize table name based on database type.
     * PostGIS uses lowercase, H2GIS uses uppercase.
     * @param connection Database connection
     * @param tableName Table name to normalize
     * @return Normalized table name
     */
    static String normalizeTableName(Connection connection, String tableName) {
        return isPostgreSQL(connection) ? tableName.toLowerCase() : tableName.toUpperCase()
    }

    /**
     * Normalize table name according to database conventions
     * - H2GIS: uppercase
     * - PostGIS: keep original (will be lowercase when queried)
     * @param tableName Original table name
     * @param connection Database connection
     * @return Normalized table name
     */
    static String normalizeTableName(String tableName, Connection connection) {
        return isPostgreSQL(connection) ? tableName : tableName.toUpperCase()
    }

    /**
     * Normalize column name returned by JDBC ResultSets.
     * PostgreSQL lowercases unquoted identifiers while H2GIS uppercases them.
     * @param connection Database connection
     * @param columnName Logical column name
     * @return Column name as exposed by ResultSet
     */
    static String normalizeResultSetColumnName(Connection connection, String columnName) {
        return isPostgreSQL(connection) ? columnName.toLowerCase() : columnName.toUpperCase()
    }

    /**
     * Get integer primary key column index for a table (cross-database compatible).
     * Works around JDBCUtilities.getIntegerPrimaryKey() case-sensitivity bug on PostgreSQL.
     * @param connection Database connection
     * @param tableName Table name (will be normalized internally)
     * @return Primary key column index (1-based), or 0 if not found
     */
    static int getIntegerPrimaryKey(Connection connection, String tableName) {
        if (isPostgreSQL(connection)) {
            // Query pg_index directly to avoid case sensitivity issues
            def stmt = connection.createStatement()
            def rs = stmt.executeQuery(
                "SELECT a.attnum " +
                "FROM pg_index i " +
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                "WHERE i.indrelid = '${tableName.toLowerCase()}'::regclass " +
                "AND i.indisprimary " +
                "AND a.atttypid IN (20, 21, 23) " + // bigint, smallint, integer
                "ORDER BY a.attnum " +
                "LIMIT 1"
            )
            int pkIndex = rs.next() ? rs.getInt("attnum") : 0
            rs.close()
            stmt.close()
            return pkIndex
        } else {
            return JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse(tableName.toUpperCase()))
        }
    }

    /**
     * Retrieve primary key column name for a table.
     * @param connection Database connection
     * @param tableName Table name
     * @return Primary key column name or empty string if none
     */
    static String getPrimaryKeyColumn(Connection connection, String tableName) {
        String schema = connection.getSchema()
        String normalizedTable = normalizeTableName(connection, tableName)

        ResultSet rs = connection.getMetaData().getPrimaryKeys(connection.getCatalog(), schema, normalizedTable)
        try {
            if (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME")
                return columnName != null ? columnName : ""
            }
            return ""
        } finally {
            rs.close()
        }
    }

    /**
     * Get SRID from table (works for both H2GIS and PostGIS)
     * @param connection Database connection
     * @param tableName Table name (already normalized)
     * @param geomColumn Geometry column name (default: 'the_geom')
     * @return SRID value
     */
    static int getTableSRID(Connection connection, String tableName, String geomColumn = 'the_geom') {
        DBTypes dbType = getDBType(connection)
        if (dbType == DBTypes.POSTGRESQL) {
            return getPostgresSRID(connection, tableName, geomColumn)
        } else {
            return GeometryTableUtilities.getSRID(connection, TableLocation.parse(tableName, dbType))
        }
    }

    /**
     * Get SRID from PostgreSQL table.
     * Tries geometry_columns metadata first, then falls back to querying actual geometries.
     * <p>
     * <b>Implementation Note:</b> This two-step approach is necessary because:
     * <ol>
     *   <li>geometry_columns is faster (no table scan) but may be outdated or missing</li>
     *   <li>Querying actual geometries is slower but always reflects current data</li>
     * </ol>
     * 
     * @param connection PostgreSQL database connection
     * @param tableName Table name (will be lowercased automatically)
     * @param geomColumn Geometry column name (will be lowercased automatically)
     * @return SRID value, or 0 if not found
     */
    private static int getPostgresSRID(Connection connection, String tableName, String geomColumn) {
        def stmt = connection.createStatement()
        
        // Step 1: Try geometry_columns metadata (fast, O(1) lookup)
        int srid = getSRIDFromMetadata(stmt, tableName, geomColumn)
        
        // Step 2: Fallback to actual geometry query (slower, requires table scan)
        if (srid == 0) {
            srid = getSRIDFromGeometry(stmt, tableName, geomColumn)
        }
        
        stmt.close()
        return srid
    }

    /**
     * Get SRID from PostgreSQL geometry_columns metadata table.
     * <p>
     * The geometry_columns table is part of the PostGIS extension and stores metadata about
     * geometry columns in the database. This is the recommended way to query SRID when available.
     * 
     * @param stmt Statement to use for query (reused to avoid creating multiple statements)
     * @param tableName Table name (lowercase)
     * @param geomColumn Geometry column name (lowercase)
     * @return SRID value, or 0 if not found in metadata
     */
    private static int getSRIDFromMetadata(def stmt, String tableName, String geomColumn) {
        def rs = stmt.executeQuery(
            "SELECT srid FROM geometry_columns " +
            "WHERE f_table_name = '${tableName.toLowerCase()}' " +
            "AND f_geometry_column = '${geomColumn.toLowerCase()}' " +
            "LIMIT 1"
        )
        int srid = rs.next() ? rs.getInt("srid") : 0
        rs.close()
        return srid
    }

    /**
     * Get SRID by querying actual geometry values in PostgreSQL table.
     * <p>
     * This is a fallback method when geometry_columns metadata is not available or unreliable.
     * Uses ST_SRID() PostGIS function to extract SRID from the first non-null geometry.
     * 
     * @param stmt Statement to use for query
     * @param tableName Table name (lowercase)
     * @param geomColumn Geometry column name (lowercase)
     * @return SRID value, or 0 if table is empty or has no geometries
     */
    private static int getSRIDFromGeometry(def stmt, String tableName, String geomColumn) {
        def rs = stmt.executeQuery(
            "SELECT ST_SRID(${geomColumn.toLowerCase()}) as srid " +
            "FROM ${tableName.toLowerCase()} " +
            "LIMIT 1"
        )
        int srid = rs.next() ? rs.getInt("srid") : 0
        rs.close()
        return srid
    }

    /**
     * Ensure SRID is properly set for a table in PostgreSQL.
     * This fixes issues with SHPRead.importTable() which doesn't properly set SRID metadata.
     * @param connection Database connection
     * @param tableName Table name
     * @param geomColumn Geometry column name (default: 'the_geom')
     * @param targetSRID Target SRID to set (if 0, will detect from existing geometries)
     */
    static void ensureSRID(Connection connection, String tableName, String geomColumn = 'the_geom', int targetSRID = 0) {
        if (!isPostgreSQL(connection)) {
            return // H2GIS handles SRID correctly
        }
        
        try {
            def stmt = connection.createStatement()
            String normalizedTable = tableName.toLowerCase()
            String normalizedColumn = geomColumn.toLowerCase()
            
            // Detect SRID from geometries if not specified
            if (targetSRID == 0) {
                targetSRID = detectSRIDFromTable(stmt, normalizedTable, normalizedColumn)
            }
            
            // Update geometries and metadata with the correct SRID
            if (targetSRID > 0) {
                updateGeometrySRID(stmt, normalizedTable, normalizedColumn, targetSRID)
                updateGeometryMetadata(connection, stmt, normalizedTable, normalizedColumn, targetSRID)
            }
            
            stmt.close()
        } catch (Exception e) {
            // SRID enforcement failed - not critical for most operations
        }
    }

    /**
     * Detect SRID from existing geometries in a PostgreSQL table.
     * <p>
     * Queries the first non-null geometry to determine its SRID. This is useful when:
     * <ul>
     *   <li>Importing shapefiles that don't set PostgreSQL metadata properly</li>
     *   <li>The targetSRID parameter to {@link #ensureSRID} is 0 (auto-detect)</li>
     *   <li>Migrating data between databases with different SRID conventions</li>
     * </ul>
     * 
     * @param stmt Statement to use for query (reused for efficiency)
     * @param tableName Normalized table name (lowercase for PostgreSQL)
     * @param geomColumn Normalized geometry column name (lowercase for PostgreSQL)
     * @return SRID value from first geometry, or 0 if no geometries found
     */
    private static int detectSRIDFromTable(def stmt, String tableName, String geomColumn) {
        def query = "SELECT ST_SRID(" + geomColumn + ") as srid " +
            "FROM " + tableName + " " +
            "WHERE " + geomColumn + " IS NOT NULL " +
            "LIMIT 1"
        def rs = stmt.executeQuery(query)
        int srid = rs.next() ? rs.getInt("srid") : 0
        rs.close()
        return srid
    }

    /**
     * Update geometry values in table to have the correct SRID.
     * <p>
     * This operation is necessary because:
     * <ol>
     *   <li>Shapefile imports often leave geometries with SRID=0</li>
     *   <li>Some operations require consistent SRID across all geometries</li>
     *   <li>Spatial joins and indexes work better with proper SRID</li>
     * </ol>
     * <p>
     * <b>Performance Note:</b> This performs a full table update, which can be slow on large tables.
     * However, it only updates rows where SRID is 0 or different from target, minimizing writes.
     * 
     * @param stmt Statement to use for update (reused for efficiency)
     * @param tableName Normalized table name (lowercase for PostgreSQL)
     * @param geomColumn Normalized geometry column name (lowercase for PostgreSQL)
     * @param targetSRID Target SRID value to set on all geometries
     */
    private static void updateGeometrySRID(def stmt, String tableName, String geomColumn, int targetSRID) {
        // Use ST_SetSRID to update geometry SRID without reprojecting
        // Only update rows that have wrong SRID to minimize writes
        def updateQuery = "UPDATE " + tableName + " " +
            "SET " + geomColumn + " = ST_SetSRID(" + geomColumn + ", " + targetSRID + ") " +
            "WHERE ST_SRID(" + geomColumn + ") = 0 OR ST_SRID(" + geomColumn + ") != " + targetSRID
        stmt.execute(updateQuery)
    }

    /**
     * Update PostgreSQL geometry_columns metadata table with correct SRID.
     * <p>
     * The geometry_columns table is a PostGIS system table that stores metadata about geometry
     * columns. This metadata is used by:
     * <ul>
     *   <li>QGIS and other GIS clients to determine coordinate system</li>
     *   <li>PostGIS spatial functions for optimization</li>
     *   <li>Database administrators for documentation</li>
     * </ul>
     * <p>
     * <b>Schema Handling:</b> If the connection has an active schema, updates only that schema's
     * metadata. Otherwise, updates the public schema (or whichever schema contains the table).
     * 
     * @param connection Database connection (for schema lookup)
     * @param stmt Statement to use for update
     * @param tableName Normalized table name (lowercase for PostgreSQL)
     * @param geomColumn Normalized geometry column name (lowercase for PostgreSQL)
     * @param targetSRID Target SRID value to set in metadata
     */
    private static void updateGeometryMetadata(Connection connection, def stmt, String tableName, String geomColumn, int targetSRID) {
        def schema = connection.getSchema()
        def metadataQuery
        
        if (schema) {
            // Schema-aware update: only update metadata for current schema
            metadataQuery = "UPDATE geometry_columns " +
                "SET srid = " + targetSRID + " " +
                "WHERE f_table_schema = '" + schema + "' " +
                "AND f_table_name = '" + tableName + "' " +
                "AND f_geometry_column = '" + geomColumn + "'"
        } else {
            // Schema-agnostic update: update any schema containing this table
            metadataQuery = "UPDATE geometry_columns " +
                "SET srid = " + targetSRID + " " +
                "WHERE f_table_name = '" + tableName + "' " +
                "AND f_geometry_column = '" + geomColumn + "'"
        }
        stmt.execute(metadataQuery)
    }

    /**
     * Get table envelope (bounding box) geometry in a cross-database compatible way.
     * On PostgreSQL, GeometryTableUtilities.getEnvelope() fails with PGobject cast error.
     * This method uses ST_Extent() with BOX parsing instead.
     * @param connection Database connection
     * @param tableName Table name
     * @param geomColumn Geometry column name (optional, auto-detected)
     * @return Geometry representing the bounding box (Polygon)
     */
    static Geometry getTableEnvelope(Connection connection, String tableName, String geomColumn = null) {
        // Auto-detect geometry column name if not provided
        if (geomColumn == null) {
            geomColumn = getGeometryColumnName(connection)
        }
        
        if (!isPostgreSQL(connection)) {
            // H2GIS - use GeometryTableUtilities
            return GeometryTableUtilities.getEnvelope(
                connection, 
                TableLocation.parse(tableName), 
                geomColumn
            )
        } else {
            // PostgreSQL - use ST_Extent() with BOX parsing
            return getPostgresTableEnvelope(connection, tableName, geomColumn)
        }
    }

    /**
     * Get table envelope from PostgreSQL using ST_Extent().
     * <p>
     * PostgreSQL's ST_Extent() aggregate function returns a BOX string representation of the
     * bounding box. This method:
     * <ol>
     *   <li>Queries ST_Extent() to get aggregated bounding box</li>
     *   <li>Parses the BOX string format into coordinates</li>
     *   <li>Constructs a JTS Polygon geometry</li>
     *   <li>Sets the SRID from table metadata</li>
     * </ol>
     * <p>
     * <b>Why not use GeometryTableUtilities?</b> The H2GIS utility class attempts to cast
     * PostgreSQL's PGobject to JTS Geometry, which fails. This implementation uses the
     * text-based ST_Extent() output instead.
     * 
     * @param connection PostgreSQL database connection
     * @param tableName Table name (will be normalized to lowercase)
     * @param geomColumn Geometry column name (will be normalized to lowercase)
     * @return Geometry representing the bounding box (Polygon), or null if table is empty
     */
    private static Geometry getPostgresTableEnvelope(Connection connection, String tableName, String geomColumn) {
        def stmt = connection.createStatement()
        String normalizedTable = tableName.toLowerCase()
        String normalizedColumn = geomColumn.toLowerCase()
        
        // Query ST_Extent() which returns BOX format string
        def query = "SELECT ST_Extent(" + normalizedColumn + ") as bbox FROM " + normalizedTable
        def rs = stmt.executeQuery(query)
        
        Geometry envelope = null
        if (rs.next()) {
            String boxStr = rs.getString("bbox")
            if (boxStr != null && boxStr.startsWith("BOX")) {
                // Parse BOX string into Polygon geometry
                envelope = parsePostgresBOX(boxStr)
                
                // Set SRID from table metadata
                int srid = getTableSRID(connection, normalizedTable, normalizedColumn)
                if (srid > 0) {
                    envelope.setSRID(srid)
                }
            }
        }
        rs.close()
        stmt.close()
        
        return envelope
    }

    /**
     * Parse PostgreSQL BOX string into JTS Geometry (Polygon).
     * <p>
     * PostgreSQL ST_Extent() returns bounding boxes in BOX format:
     * <pre>BOX(minx miny,maxx maxy)</pre>
     * <p>
     * This method:
     * <ol>
     *   <li>Removes "BOX(" prefix and ")" suffix</li>
     *   <li>Splits coordinates by comma separator</li>
     *   <li>Parses min/max X and Y values</li>
     *   <li>Constructs a closed Polygon (5 coordinates)</li>
     * </ol>
     * <p>
     * <b>Coordinate Order:</b> The polygon is created counter-clockwise starting from bottom-left:
     * (minX, minY) → (maxX, minY) → (maxX, maxY) → (minX, maxY) → (minX, minY)
     * 
     * @param boxStr BOX string from PostgreSQL ST_Extent() (e.g., "BOX(0 0,100 100)")
     * @return Polygon geometry representing the bounding box (SRID not set, must be set by caller)
     */
    private static Geometry parsePostgresBOX(String boxStr) {
        // Remove "BOX(" prefix and ")" suffix
        String coords = boxStr.substring(4, boxStr.length() - 1)
        
        // Split by comma to get min and max points
        def parts = coords.split(',')
        def min = parts[0].trim().split(' ')  // "minx miny"
        def max = parts[1].trim().split(' ')  // "maxx maxy"
        
        // Parse coordinate values
        double minX = Double.parseDouble(min[0])
        double minY = Double.parseDouble(min[1])
        double maxX = Double.parseDouble(max[0])
        double maxY = Double.parseDouble(max[1])
        
        // Create Polygon from envelope coordinates (counter-clockwise)
        GeometryFactory gf = new GeometryFactory()
        Coordinate[] polyCoords = [
            new Coordinate(minX, minY),  // Bottom-left
            new Coordinate(maxX, minY),  // Bottom-right
            new Coordinate(maxX, maxY),  // Top-right
            new Coordinate(minX, maxY),  // Top-left
            new Coordinate(minX, minY)   // Close the ring
        ] as Coordinate[]
        
        return gf.createPolygon(polyCoords)
    }

    /**
     * Get geometry column names from table (works for both H2GIS and PostGIS)
     * @param connection Database connection
     * @param tableName Table name (already normalized)
     * @return List of geometry column names (in uppercase)
     */
    static List<String> getGeometryColumns(Connection connection, String tableName) {
        DBTypes dbType = getDBType(connection)
        if (dbType == DBTypes.POSTGRESQL) {
            List<String> geomFields = []
            def stmt = connection.createStatement()
            def rs = stmt.executeQuery(
                "SELECT f_geometry_column FROM geometry_columns " +
                "WHERE f_table_name = '${tableName.toLowerCase()}'"
            )
            while (rs.next()) {
                // Normalize to uppercase for consistency with H2GIS
                geomFields.add(rs.getString("f_geometry_column").toUpperCase())
            }
            rs.close()
            stmt.close()
            return geomFields
        } else {
            return GeometryTableUtilities.getGeometryColumnNames(connection, TableLocation.parse(tableName, dbType))
        }
    }

    /**
     * Get primary key index from table (works for both H2GIS and PostGIS)
     * @param connection Database connection
     * @param tableName Table name (already normalized)
     * @return Primary key column index (1-based), 0 if no PK found
     */
    static int getPrimaryKeyIndex(Connection connection, String tableName) {
        DBTypes dbType = getDBType(connection)
        if (dbType == DBTypes.POSTGRESQL) {
            def stmt = connection.createStatement()
            def rs = stmt.executeQuery("""
                SELECT a.attnum FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary 
                AND i.indrelid = (
                    SELECT oid FROM pg_class 
                    WHERE relname = '${tableName.toLowerCase()}' 
                    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
                )
                LIMIT 1
            """)
            int pkIndex = rs.next() ? rs.getInt(1) : 0
            rs.close()
            stmt.close()
            return pkIndex
        } else {
            return JDBCUtilities.getIntegerPrimaryKey(connection, TableLocation.parse(tableName, dbType))
        }
    }

    /**
     * Create spatial index on geometry column
     * Automatically normalizes table and column names for the target database.
     * @param connection Database connection
     * @param tableName Table name
     * @param geomColumn Geometry column name (default: 'the_geom')
     * @param indexName Optional index name (auto-generated if not provided)
     */
    static void createSpatialIndex(Connection connection, String tableName, String geomColumn = 'the_geom', String indexName = null) {
        if (!indexName) {
            indexName = "${tableName}_INDEX".toString()
        }
        
        // Normalize names based on database type
        boolean isPostGIS = isPostgreSQL(connection)
        String normalizedTableName = isPostGIS ? tableName.toLowerCase() : tableName.toUpperCase()
        String normalizedGeomColumn = isPostGIS ? geomColumn.toLowerCase() : geomColumn.toUpperCase()
        
        def stmt = connection.createStatement()
        if (isPostGIS) {
            stmt.execute("CREATE INDEX IF NOT EXISTS ${indexName} ON ${normalizedTableName} USING GIST (${normalizedGeomColumn})")
        } else {
            stmt.execute("CREATE SPATIAL INDEX IF NOT EXISTS ${indexName} ON ${normalizedTableName}(${normalizedGeomColumn})")
        }
        stmt.close()
    }

    /**
     * Add an auto-increment primary key column in a database agnostic way.
     * @param connection Database connection
     * @param tableName Table name
     * @param columnName Column name to create (default: pk)
     */
    static void addAutoIncrementPrimaryKey(Connection connection, String tableName, String columnName = 'pk') {
        def stmt = connection.createStatement()
        try {
            if (isPostgreSQL(connection)) {
                stmt.execute("ALTER TABLE ${tableName} ADD COLUMN ${columnName} SERIAL PRIMARY KEY")
            } else {
                stmt.execute("ALTER TABLE ${tableName} ADD COLUMN ${columnName} INT AUTO_INCREMENT PRIMARY KEY")
            }
        } finally {
            stmt.close()
        }
    }

    /**
     * Get SQL for setting SRID on geometry column
     * @param connection Database connection
     * @param geomColumn Geometry column name
     * @param srid SRID value
     * @return SQL expression to set SRID
     */
    static String getSRIDSetExpression(Connection connection, String geomColumn, int srid) {
        return isPostgreSQL(connection) 
            ? "ST_SetSRID(${geomColumn}, ${srid})"
            : "ST_SetSRID(${geomColumn}, ${srid})"
    }

    /**
     * Get SQL for geometry type detection
     * @param connection Database connection
     * @param geomColumn Geometry column name
     * @return SQL expression to get geometry type
     */
    static String getGeometryTypeExpression(Connection connection, String geomColumn) {
        return isPostgreSQL(connection)
            ? "ST_GeometryType(${geomColumn})"
            : "ST_GeometryType(${geomColumn})"
    }

    /**
     * Prepare a geometry parameter for use with Groovy Sql named parameters.
     * Returns an expression fragment and parameters map suited for the connection type.
     * @param connection Database connection
     * @param geometry Geometry instance (must have SRID set when using PostgreSQL)
     * @param parameterBase Base name for the parameter (e.g. "fenceGeom")
     * @return GeometryParameter containing SQL expression and parameter map
     */
    static GeometryParameter prepareGeometryParameter(Connection connection, Geometry geometry, String parameterBase) {
        GeometryParameter result = new GeometryParameter(expression: 'NULL', parameters: [:])
        if (geometry == null) {
            return result
        }

        if (isPostgreSQL(connection)) {
            int srid = geometry.getSRID()
            result.expression = "ST_SetSRID(ST_GeomFromText(:${parameterBase}Wkt), :${parameterBase}Srid)"
            result.parameters[(parameterBase + 'Wkt')] = geometry.toText()
            result.parameters[(parameterBase + 'Srid')] = srid
        } else {
            result.expression = ":${parameterBase}"
            result.parameters[parameterBase] = geometry
        }
        return result
    }

    /**
     * Read geometry from a ResultSet in a cross-database manner.
     * @param connection Database connection
     * @param rs ResultSet positioned on a row
     * @param columnName Column name to read (logical name)
     * @return Geometry instance or null if column is null
     */
    static Geometry getGeometryFromResultSet(Connection connection, ResultSet rs, String columnName) {
        String resultSetColumn = normalizeResultSetColumnName(connection, columnName)
        Object geomObj = rs.getObject(resultSetColumn)
        if (geomObj == null) {
            return null
        }

        if (geomObj instanceof Geometry) {
            return geomObj as Geometry
        }

        if (isPostgreSQL(connection)) {
            String hexValue
            if (geomObj instanceof PGobject) {
                hexValue = ((PGobject) geomObj).getValue()
            } else {
                hexValue = geomObj.toString()
            }
            if (!hexValue) {
                return null
            }
            return new WKBReader().read(hexValue.decodeHex())
        }

        throw new IllegalArgumentException("Unsupported geometry object type: " + geomObj.getClass().getName())
    }

    /**
     * Execute DROP TABLE IF EXISTS in a cross-database compatible way
     * @param connection Database connection
     * @param tableName Table name to drop
     */
    static void dropTableIfExists(Connection connection, String tableName) {
        def stmt = connection.createStatement()
        stmt.execute("DROP TABLE IF EXISTS ${tableName}")
        stmt.close()
    }

    /**
     * Get the geometry column name for the database type.
     * PostgreSQL: the_geom (lowercase)
     * H2GIS: THE_GEOM (uppercase)
     * 
     * @param connection Database connection
     * @return Geometry column name
     */
    static String getGeometryColumnName(Connection connection) {
        return isPostgreSQL(connection) ? "the_geom" : "THE_GEOM"
    }

    /**
     * Normalize column name based on database type.
     * PostgreSQL: converts to lowercase
     * H2GIS: converts to uppercase
     * 
     * @param connection Database connection
     * @param columnName Column name to normalize
     * @return Normalized column name
     */
    static String normalizeColumnName(Connection connection, String columnName) {
        if (columnName == null) return null
        return isPostgreSQL(connection) ? columnName.toLowerCase() : columnName.toUpperCase()
    }

    /**
     * Execute DROP TABLE IF EXISTS for multiple tables in a cross-database compatible way
     * @param connection Database connection
     * @param tableNames List of table names to drop
     */
    static void dropTablesIfExist(Connection connection, String... tableNames) {
        def stmt = connection.createStatement()
        tableNames.each { tableName ->
            stmt.execute("DROP TABLE IF EXISTS ${tableName}")
        }
        stmt.close()
    }

    /**
     * Create a regular grid of points covering the provided geometry in a cross-database compatible way.
     * Replaces H2GIS-specific ST_MakeGridPoints/ST_UpdateZ usage by delegating to database-specific SQL here.
     * @param connection Database connection
     * @param tableName Target table name (will be normalized internally)
     * @param fenceGeometry Geometry delimiting the grid extent (required)
     * @param step Grid spacing (same value for X/Y)
     * @param height Receiver height (applied to Z coordinate)
     * @param srid Target SRID (falls back to fence SRID when zero)
     * @param geomColumn Geometry column name (defaults to database convention)
     * @param idColName Column storing column index (defaults to ID_COL)
     * @param idRowName Column storing row index (defaults to ID_ROW)
     */
    static void createGridPointsTable(Connection connection,
                                      String tableName,
                                      Geometry fenceGeometry,
                                      double step,
                                      double height,
                                      int srid,
                                      String geomColumn = null,
                                      String idColName = "ID_COL",
                                      String idRowName = "ID_ROW") {
        if (fenceGeometry == null) {
            throw new IllegalArgumentException("Fence geometry is required to build the grid")
        }

        boolean isPostGIS = isPostgreSQL(connection)
        String normalizedTable = normalizeTableName(connection, tableName)
        String targetGeomColumn = geomColumn ?: getGeometryColumnName(connection)
        String normalizedGeom = normalizeColumnName(connection, targetGeomColumn)
        String normalizedIdCol = normalizeColumnName(connection, idColName)
        String normalizedIdRow = normalizeColumnName(connection, idRowName)

        int effectiveSRID = srid > 0 ? srid : fenceGeometry.getSRID()
        if (effectiveSRID < 0) {
            effectiveSRID = 0
        }

        String fenceWkt = escapeSqlLiteral(fenceGeometry.toText())
        String stepLiteral = formatDouble(step)
        String heightLiteral = formatDouble(height)

        def stmt = connection.createStatement()
        try {
            if (isPostGIS) {
                Envelope envelope = fenceGeometry.getEnvelopeInternal()
                String minX = formatDouble(envelope.getMinX())
                String maxX = formatDouble(envelope.getMaxX())
                String minY = formatDouble(envelope.getMinY())
                String maxY = formatDouble(envelope.getMaxY())

                StringBuilder createQuery = new StringBuilder()
                createQuery.append("CREATE TABLE ")
                        .append(normalizedTable)
                        .append(" AS SELECT ")
                        .append("ST_SetSRID(ST_MakePoint(x, y, ")
                        .append(heightLiteral)
                        .append("), ")
                        .append(effectiveSRID)
                        .append(") AS ")
                        .append(normalizedGeom)
                        .append(", FLOOR((x - ")
                        .append(minX)
                        .append(") / ")
                        .append(stepLiteral)
                        .append(")::integer AS ")
                        .append(normalizedIdCol)
                        .append(", FLOOR((y - ")
                        .append(minY)
                        .append(") / ")
                        .append(stepLiteral)
                        .append(")::integer AS ")
                        .append(normalizedIdRow)
                        .append(" FROM generate_series(")
                        .append(minX)
                        .append(", ")
                        .append(maxX)
                        .append(", ")
                        .append(stepLiteral)
                        .append(") AS x, generate_series(")
                        .append(minY)
                        .append(", ")
                        .append(maxY)
                        .append(", ")
                        .append(stepLiteral)
                        .append(") AS y WHERE ST_Intersects(ST_Force2D(ST_SetSRID(ST_MakePoint(x, y), ")
                        .append(effectiveSRID)
                        .append(")), ST_Force2D(ST_GeomFromText('")
                        .append(fenceWkt)
                        .append("', ")
                        .append(effectiveSRID)
                        .append(")))")

                stmt.execute(createQuery.toString())
                stmt.execute("ALTER TABLE " + normalizedTable + " ADD COLUMN pk SERIAL PRIMARY KEY")
            } else {
                StringBuilder createQuery = new StringBuilder()
                createQuery.append("CREATE TABLE ")
                        .append(normalizedTable)
                        .append("(")
                        .append(normalizedGeom)
                        .append(" GEOMETRY, ")
                        .append(normalizedIdCol)
                        .append(" INTEGER, ")
                        .append(normalizedIdRow)
                        .append(" INTEGER) AS SELECT ST_SetSRID(ST_UpdateZ(THE_GEOM, ")
                        .append(heightLiteral)
                        .append("), ")
                        .append(effectiveSRID)
                        .append(") AS ")
                        .append(normalizedGeom)
                        .append(", ID_COL AS ")
                        .append(normalizedIdCol)
                        .append(", ID_ROW AS ")
                        .append(normalizedIdRow)
                        .append(" FROM ST_MakeGridPoints(ST_GeomFromText('")
                        .append(fenceWkt)
                        .append("'), ")
                        .append(stepLiteral)
                        .append(", ")
                        .append(stepLiteral)
                        .append(")")

                stmt.execute(createQuery.toString())
                stmt.execute("ALTER TABLE " + normalizedTable + " ADD COLUMN PK INT AUTO_INCREMENT PRIMARY KEY")
            }
        } finally {
            stmt.close()
        }
    }

    /**
     * Normalize table name for passing to H2GIS utility methods.
     * These methods query database metadata which is case-sensitive on PostGIS.
     * Use this when calling JDBCUtilities, GeometryTableUtilities, etc.
     * @param connection Database connection
     * @param tableName Table name to normalize
     * @return Normalized table name for utility calls
     */
    static String normalizeTableNameForUtilities(Connection connection, String tableName) {
        if (tableName == null || tableName.isEmpty()) return tableName
        return isPostgreSQL(connection) ? tableName.toLowerCase() : tableName.toUpperCase()
    }
    
    private static String escapeSqlLiteral(String value) {
        return value != null ? value.replace("'", "''") : null
    }

    private static String formatDouble(double value) {
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString()
    }

    /**
     * Import SHP file and ensure SRID is properly set for PostgreSQL.
     * This is a wrapper around SHPRead.importTable() that fixes the PostGIS metadata issue.
     * @param connection Database connection
     * @param shpPath Path to SHP file
     * @return Table name that was created
     */
    static String importShapefileWithSRID(Connection connection, String shpPath) {
        SHPRead.importTable(connection, shpPath)
        
        File shpFile = new File(shpPath)
        String tableName = shpFile.getName().replaceAll(/(?i)\.shp$/, '')
        
        // Fix SRID for PostgreSQL (H2GIS doesn't need this)
        ensureSRID(connection, tableName, 'the_geom')
        
        return tableName
    }
    
    /**
     * Import SHP file with forceImport flag and ensure SRID is properly set for PostgreSQL.
     * @param connection Database connection
     * @param shpPath Path to SHP file
     * @param forceImport Force import flag
     * @return Table name that was created
     */
    static String importShapefileWithSRID(Connection connection, String shpPath, ValueBoolean forceImport) {
        SHPRead.importTable(connection, shpPath, forceImport)
        
        File shpFile = new File(shpPath)
        String tableName = shpFile.getName().replaceAll(/(?i)\.shp$/, '')
        
        // Fix SRID for PostgreSQL (H2GIS doesn't need this)
        ensureSRID(connection, tableName, 'the_geom')
        
        return tableName
    }
    
    /**
     * Import SHP file with custom table name and forceImport flag, ensuring SRID is properly set for PostgreSQL.
     * @param connection Database connection
     * @param shpPath Path to SHP file
     * @param tableName Custom table name
     * @param forceImport Force import flag
     * @return Table name that was created
     */
    static String importShapefileWithSRID(Connection connection, String shpPath, String tableName, ValueBoolean forceImport) {
        SHPRead.importTable(connection, shpPath, tableName, forceImport)
        
        // Fix SRID for PostgreSQL (H2GIS doesn't need this)
        ensureSRID(connection, tableName, 'the_geom')
        
        return tableName
    }

    /**
     * Validate that a table has a valid SRID (spatial reference system).
     * Returns false if SRID is 0 or cannot be determined.
     * @param connection Database connection
     * @param tableName Table name to check
     * @param geomColumn Geometry column name (default: 'the_geom')
     * @return true if table has valid SRID (>0), false otherwise
     */
    static boolean validateTableSRID(Connection connection, String tableName, String geomColumn = 'the_geom') {
        try {
            int srid = getTableSRID(connection, tableName, geomColumn)
            return srid > 0
        } catch (Exception e) {
            return false
        }
    }

    /**
     * Ensure a table has a valid SRID, throwing an exception if it doesn't and no default is provided.
     * If defaultSRID is provided and table has no SRID, it will be set.
     * @param connection Database connection
     * @param tableName Table name to check/fix
     * @param geomColumn Geometry column name (default: 'the_geom')
     * @param defaultSRID Default SRID to use if table has none (0 = throw exception)
     * @throws IllegalArgumentException if table has no SRID and defaultSRID is 0
     */
    static void ensureTableHasSRID(Connection connection, String tableName, String geomColumn = 'the_geom', int defaultSRID = 0) {
        if (!validateTableSRID(connection, tableName, geomColumn)) {
            if (defaultSRID == 0) {
                throw new IllegalArgumentException("Error : The table ${tableName} does not have an associated spatial reference system. (missing prj file on import ?)")
            }
            ensureSRID(connection, tableName, geomColumn, defaultSRID)
        }
    }

    /**
     * Create a SQL INSERT statement for 3D points that works across both databases.
     * PostgreSQL requires ST_MakePoint(x,y,z) while H2GIS accepts Geometry objects.
     * Returns the SQL query string and number of parameters per point.
     * @param connection Database connection
     * @param tableName Target table name
     * @param geomColumn Geometry column name
     * @param srid SRID for the geometries
     * @param otherColumns List of other column names (besides geom column)
     * @return Map with 'query' (String) and 'geomParamCount' (int: 3 for PG coords, 1 for H2 object)
     */
    static Map<String, Object> prepare3DPointInsertStatement(Connection connection, String tableName, 
                                                              String geomColumn, int srid, List<String> otherColumns) {
        StringBuilder query = new StringBuilder("INSERT INTO ").append(tableName).append(" (")
        query.append(geomColumn)
        otherColumns.each { col -> query.append(", ").append(col) }
        query.append(") VALUES (")
        
        int geomParamCount
        if (isPostgreSQL(connection)) {
            // PostgreSQL: ST_SetSRID(ST_MakePoint(x, y, z), srid)
            query.append("ST_SetSRID(ST_MakePoint(?, ?, ?), ").append(srid).append(")")
            geomParamCount = 3
        } else {
            // H2GIS: accepts Geometry object directly
            query.append("?")
            geomParamCount = 1
        }
        
        // Add placeholders for other columns
        otherColumns.each { query.append(", ?") }
        query.append(")")
        
        return [query: query.toString(), geomParamCount: geomParamCount]
    }

    /**
     * Add parameters to a PreparedStatement batch for a 3D point insertion.
     * Handles the difference between PostgreSQL (3 coordinate params) and H2GIS (1 Geometry param).
     * @param connection Database connection
     * @param ps PreparedStatement
     * @param coordinate 3D coordinate to insert
     * @param factory GeometryFactory with SRID set
     * @param otherValues Additional column values (in order matching otherColumns from prepare statement)
     */
    static void addBatch3DPoint(Connection connection, PreparedStatement ps, Coordinate coordinate,
                                GeometryFactory factory, List<Object> otherValues) {
        int paramIndex = 1
        
        if (isPostgreSQL(connection)) {
            // PostgreSQL: set x, y, z separately
            ps.setDouble(paramIndex++, coordinate.x)
            ps.setDouble(paramIndex++, coordinate.y)
            ps.setDouble(paramIndex++, coordinate.z)
        } else {
            // H2GIS: set Geometry object
            ps.setObject(paramIndex++, factory.createPoint(coordinate))
        }
        
        // Set other column values
        otherValues.each { value ->
            ps.setObject(paramIndex++, value)
        }
        
        ps.addBatch()
    }
}

