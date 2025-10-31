/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research
 * and education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE
 * provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */
package org.noise_planet.noisemodelling.jdbc.utils;

import org.h2gis.utilities.GeometryTableUtilities;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.Tuple;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTWriter;

import java.sql.*;
import java.util.List;
import java.util.Locale;

/**
 * Utility helper for handling geometry parameters across H2GIS and PostGIS databases.
 */
public final class GeometrySqlHelper {

    private static final WKTWriter WKT_WRITER = new WKTWriter(3);

    private GeometrySqlHelper() {
        // Utility class
    }

    /**
     * @param dbType Database type
     * @return true if the target database is PostgreSQL/PostGIS
     */
    public static boolean isPostgreSQL(DBTypes dbType) {
    return dbType == DBTypes.POSTGRESQL || dbType == DBTypes.POSTGIS;
    }

    /**
     * Build the spatial predicate used for bounding box intersection queries.
     * Must be paired with setGeometryParameter() to bind the parameters.
     * @param connection Active JDBC connection to detect database type
     * @param columnIdentifier Geometry column identifier (already quoted if necessary)
     * @return Predicate string ready to be appended to a SQL WHERE clause
     * @throws SQLException if database type cannot be determined
     */
    public static String buildEnvelopePredicate(Connection connection, String columnIdentifier) throws SQLException {
        DBTypes dbType = DBUtils.getDBType(resolveConnection(connection));
        if (isPostgreSQL(dbType)) {
            return columnIdentifier + " && ST_SetSRID(ST_GeomFromText(?), ?)";
        }
        return columnIdentifier + " && ?";
    }

    /**
     * Set a geometry parameter on a prepared statement in a database agnostic way.
     * @param ps Prepared statement
     * @param parameterIndex Index (1-based) of the parameter to set
     * @param geometry Geometry value (must not be null)
     * @param dbType Database type
     * @return Next available parameter index after the geometry parameter(s)
     * @throws SQLException If the geometry value cannot be set
     */
    public static int setGeometryParameter(PreparedStatement ps, int parameterIndex, Geometry geometry, DBTypes dbType) throws SQLException {
        // Use provided dbType if available, otherwise detect from connection
        DBTypes actualDbType = dbType;
        if (actualDbType == null) {
            Connection conn = ps.getConnection();
            actualDbType = DBUtils.getDBType(resolveConnection(conn));
        }
        
        if (geometry == null) {
            if (isPostgreSQL(actualDbType)) {
                ps.setNull(parameterIndex++, Types.VARCHAR);
                ps.setNull(parameterIndex++, Types.INTEGER);
            } else {
                ps.setNull(parameterIndex++, Types.OTHER);
            }
            return parameterIndex;
        }
        if (isPostgreSQL(actualDbType)) {
            ps.setString(parameterIndex++, WKT_WRITER.write(geometry));
            int srid = geometry.getSRID();
            if (srid <= 0 && geometry.getFactory() != null) {
                srid = geometry.getFactory().getSRID();
            }
            ps.setInt(parameterIndex++, srid);
        } else {
            ps.setObject(parameterIndex++, geometry);
        }
        return parameterIndex;
    }

    /**
     * @param dbType Database type
     * @return SQL expression to be used when inserting geometry values via prepared statements
     */
    public static String geometryInsertExpression(DBTypes dbType) {
        return isPostgreSQL(dbType) ? "ST_SetSRID(ST_GeomFromText(?), ?)" : "?";
    }

    /**
     * Read a geometry value from a ResultSet column in a database agnostic way.
     * PostgreSQL returns PGobject which must be converted via WKB.
     * @param rs ResultSet positioned at a valid row
     * @param columnName Name of the geometry column to read
     * @param dbType Database type
     * @return Geometry value, or null if the column value is NULL
     * @throws SQLException If the geometry value cannot be read
     */
    public static Geometry getGeometry(ResultSet rs, String columnName, DBTypes dbType) throws SQLException {
        Object geomObj = rs.getObject(columnName);
        if (geomObj == null) {
            return null;
        }
        
        // Check if it's already a JTS Geometry (H2GIS)
        if (geomObj instanceof Geometry) {
            return (Geometry) geomObj;
        }
        
        // Otherwise, assume it's PostgreSQL PGobject and parse as WKB
        try {
            String hexWkb = geomObj.toString();
            if (hexWkb == null || hexWkb.isEmpty()) {
                return null;
            }
            byte[] wkb = hexStringToByteArray(hexWkb);
            return new WKBReader().read(wkb);
        } catch (Exception e) {
            throw new SQLException("Failed to parse geometry from column " + columnName + 
                    " (type: " + geomObj.getClass().getName() + ")", e);
        }
    }

    private static byte[] hexStringToByteArray(String hexString) {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Retrieve the SRID associated with the first geometry column of the provided table location.
     * On PostgreSQL the metadata may report SRID=0 when tables are created without an explicit SRID,
     * therefore we first query {@code geometry_columns} and, if inconclusive, inspect the actual data.
     * @param connection Active JDBC connection (may be wrapped)
     * @param tableLocation Table identifier including optional schema/catalog information
     * @return SRID value or 0 if none could be determined
     * @throws SQLException If an error occurs while querying database metadata
     */
    public static int getTableSRID(Connection connection, TableLocation tableLocation) throws SQLException {
        return getTableSRID(connection, tableLocation, null);
    }

    /**
     * Retrieve the SRID associated with a specific geometry column.
     * @param connection Active JDBC connection (may be wrapped)
     * @param tableLocation Table identifier including optional schema/catalog information
     * @param geometryColumn Geometry column name (optional). When {@code null}, the first geometry column is used.
     * @return SRID value or 0 if none could be determined
     * @throws SQLException If an error occurs while querying database metadata
     */
    public static int getTableSRID(Connection connection, TableLocation tableLocation, String geometryColumn) throws SQLException {
        Connection targetConnection = resolveConnection(connection);
        DBTypes dbType = DBUtils.getDBType(targetConnection);
        if (!isPostgreSQL(dbType)) {
            return GeometryTableUtilities.getSRID(targetConnection, tableLocation);
        }

        String schema = tableLocation.getSchema();
        if (schema == null || schema.isEmpty()) {
            try (Statement st = targetConnection.createStatement(); ResultSet rs = st.executeQuery("SELECT current_schema()")) {
                if (rs.next()) {
                    schema = rs.getString(1);
                }
            }
        }
        if (schema != null && schema.isEmpty()) {
            schema = null;
        }

        String table = tableLocation.getTable();
        if (table == null || table.isEmpty()) {
            return 0;
        }

        String column = geometryColumn;
        if (column == null || column.isEmpty()) {
            List<String> geometryColumns = GeometryTableUtilities.getGeometryColumnNames(targetConnection, tableLocation);
            if (geometryColumns.isEmpty()) {
                return 0;
            }
            column = geometryColumns.get(0);
        }

        String schemaLower = schema == null ? null : schema.toLowerCase(Locale.ROOT);
        String tableLower = table.toLowerCase(Locale.ROOT);
        String columnLower = column.toLowerCase(Locale.ROOT);

        StringBuilder metadataSql = new StringBuilder("SELECT srid FROM geometry_columns WHERE ");
        if (schemaLower != null) {
            metadataSql.append("f_table_schema = ? AND ");
        }
        metadataSql.append("f_table_name = ? AND f_geometry_column = ? LIMIT 1");

        try (PreparedStatement ps = targetConnection.prepareStatement(metadataSql.toString())) {
            int parameterIndex = 1;
            if (schemaLower != null) {
                ps.setString(parameterIndex++, schemaLower);
            }
            ps.setString(parameterIndex++, tableLower);
            ps.setString(parameterIndex, columnLower);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int srid = rs.getInt(1);
                    if (srid > 0) {
                        return srid;
                    }
                }
            }
        }

        StringBuilder qualifiedTable = new StringBuilder();
        String columnIdentifier;
        if (isPostgreSQL(dbType)) {
            if (schemaLower != null) {
                qualifiedTable.append(schemaLower).append(".");
            }
            qualifiedTable.append(tableLower);
            columnIdentifier = columnLower;
        } else {
            if (schema != null) {
                qualifiedTable.append(TableLocation.quoteIdentifier(schema, dbType)).append(".");
            }
            qualifiedTable.append(TableLocation.quoteIdentifier(table, dbType));
            columnIdentifier = TableLocation.quoteIdentifier(column, dbType);
        }
        String sridSql = "SELECT ST_SRID(" + columnIdentifier + ") AS srid FROM " + qualifiedTable +
                " WHERE " + columnIdentifier + " IS NOT NULL LIMIT 1";

        try (Statement st = targetConnection.createStatement(); ResultSet rs = st.executeQuery(sridSql)) {
            if (rs.next()) {
                return rs.getInt("srid");
            }
        }
        return 0;
    }

    /**
     * Retrieve the envelope of the first geometry column found in the provided table location.
     * Uses ST_Extent on PostgreSQL to avoid casting PGobject instances to JTS geometries.
     * @param connection Target database connection
     * @param tableLocation Table location including schema/catalog if any
     * @return Envelope of the geometry column, or {@code null} if no geometry data is present
     * @throws SQLException if the envelope cannot be retrieved
     */
    public static Envelope getTableEnvelope(Connection connection, TableLocation tableLocation) throws SQLException {
        Connection targetConnection = resolveConnection(connection);
        DBTypes dbType = DBUtils.getDBType(targetConnection);
        if (!isPostgreSQL(dbType)) {
            Geometry envelopeGeometry = GeometryTableUtilities.getEnvelope(targetConnection, tableLocation);
            return envelopeGeometry == null ? null : envelopeGeometry.getEnvelopeInternal();
        }

        List<String> geometryColumns = GeometryTableUtilities.getGeometryColumnNames(targetConnection, tableLocation);
        if (geometryColumns.isEmpty()) {
            return null;
        }

        String geomIdentifier = TableLocation.quoteIdentifier(geometryColumns.get(0), dbType);
        String sql = "SELECT ST_XMin(extent), ST_YMin(extent), ST_XMax(extent), ST_YMax(extent) " +
                "FROM (SELECT ST_Extent(" + geomIdentifier + ") AS extent FROM " + tableLocation + ") AS bounds";

        try (Statement st = targetConnection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                Object xminObj = rs.getObject(1);
                if (xminObj == null) {
                    return null;
                }
                double xmin = ((Number) xminObj).doubleValue();
                double ymin = ((Number) rs.getObject(2)).doubleValue();
                double xmax = ((Number) rs.getObject(3)).doubleValue();
                double ymax = ((Number) rs.getObject(4)).doubleValue();
                return new Envelope(xmin, xmax, ymin, ymax);
            }
        }
        return null;
    }

    /**
     * Retrieve the integer primary key name and index for the given table.
     * Falls back to a PostgreSQL-specific metadata query when the generic helper fails.
     * @param connection Active JDBC connection (may be wrapped)
     * @param tableLocation Table identifier
     * @return Tuple containing the primary key column name and its index
     * @throws SQLException if the primary key cannot be determined
     */
    public static Tuple<String, Integer> getIntegerPrimaryKey(Connection connection, TableLocation tableLocation) throws SQLException {
        Connection targetConnection = resolveConnection(connection);
        DBTypes dbType = DBUtils.getDBType(targetConnection);
        if (isPostgreSQL(dbType)) {
            Tuple<String, Integer> pk = getPostgreSQLPrimaryKey(targetConnection, tableLocation);
            if (pk != null) {
                return pk;
            }
        }
        return JDBCUtilities.getIntegerPrimaryKeyNameAndIndex(connection, tableLocation);
    }

    /**
     * Retrieve just the integer primary key column index for the given table.
     * @param connection Active JDBC connection (may be wrapped)
     * @param tableLocation Table identifier
     * @return Column index of the primary key (1-based), or 0 if not found
     * @throws SQLException if an error occurs
     */
    public static int getIntegerPrimaryKeyIndex(Connection connection, TableLocation tableLocation) throws SQLException {
        Connection targetConnection = resolveConnection(connection);
        DBTypes dbType = DBUtils.getDBType(targetConnection);
        if (isPostgreSQL(dbType)) {
            Tuple<String, Integer> pk = getPostgreSQLPrimaryKey(targetConnection, tableLocation);
            if (pk != null) {
                return pk.second();
            }
            return 0;
        }
        return JDBCUtilities.getIntegerPrimaryKey(connection, tableLocation);
    }

    private static Tuple<String, Integer> getPostgreSQLPrimaryKey(Connection connection, TableLocation tableLocation) throws SQLException {
        String schema = tableLocation.getSchema();
        if (schema == null || schema.isEmpty()) {
            try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery("SELECT current_schema()")) {
                if (rs.next()) {
                    schema = rs.getString(1);
                }
            }
        }
        if (schema == null || schema.isEmpty()) {
            schema = "public";
        }
        String table = tableLocation.getTable();
        if (table == null || table.isEmpty()) {
            return null;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        table = table.toLowerCase(Locale.ROOT);
        String sql = "SELECT a.attname, a.attnum " +
                "FROM pg_index i " +
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                "JOIN pg_class c ON c.oid = i.indrelid " +
                "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                "WHERE i.indisprimary AND n.nspname = ? AND c.relname = ? " +
                "ORDER BY a.attnum LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Tuple<>(rs.getString(1), rs.getInt(2));
                }
            }
        }
        return null;
    }

    public static DBTypes resolveDbType(Connection connection) throws SQLException {
        Connection target = resolveConnection(connection);
        return DBUtils.getDBType(target);
    }

    public static Connection resolveConnection(Connection connection) {
        Connection target = tryUnwrap(connection, Connection.class);
        try {
            Class<?> pgClass = Class.forName("org.postgresql.PGConnection");
            target = tryUnwrap(target, pgClass);
        } catch (ClassNotFoundException ignored) {
            // PostgreSQL driver not available; keep current connection
        }
        return target;
    }

    private static Connection tryUnwrap(Connection connection, Class<?> iface) {
        if (connection == null) {
            return null;
        }
        if (connection instanceof Wrapper) {
            try {
                Object unwrapped = ((Wrapper) connection).unwrap(iface);
                if (unwrapped instanceof Connection) {
                    return (Connection) unwrapped;
                }
            } catch (SQLException ignored) {
                // Ignore and fall back to provided connection
            }
        }
        return connection;
    }
}
