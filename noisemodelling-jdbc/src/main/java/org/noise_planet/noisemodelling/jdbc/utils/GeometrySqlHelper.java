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
import org.h2gis.utilities.SpatialResultSet;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.Tuple;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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
     * 
     * Uses the &amp;&amp; operator with ST_SetSRID(ST_GeomFromText(?), ?) which works
     * on both H2GIS and PostGIS for spatial index optimization.
     * 
     * Must be paired with setGeometryParameter() to bind the WKT and SRID parameters.
     * 
     * <p>Example usage:</p>
     * <pre>
     * String predicate = GeometrySqlHelper.buildEnvelopePredicate(conn, "the_geom");
     * String sql = "SELECT * FROM table WHERE " + predicate;
     * PreparedStatement ps = conn.prepareStatement(sql);
     * GeometrySqlHelper.setGeometryParameter(ps, 1, envelope, dbType);
     * </pre>
     * 
     * @param connection Active JDBC connection to detect database type
     * @param columnIdentifier Geometry column identifier (already quoted if necessary)
     * @return Predicate string "column &amp;&amp; ST_SetSRID(ST_GeomFromText(?), ?)"
     * @throws SQLException if database type cannot be determined
     */
    public static String buildEnvelopePredicate(Connection connection, String columnIdentifier) throws SQLException {
        // Both H2GIS and PostGIS support this syntax with spatial index optimization
        return columnIdentifier + " && ST_SetSRID(ST_GeomFromText(?), ?)";
    }

    /**
     * Set a geometry parameter on a prepared statement using WKT format.
     * 
     * This method uses WKT (Well-Known Text) format with ST_GeomFromText/ST_SetSRID,
     * which works on both H2GIS and PostGIS, eliminating the need for database-specific code paths.
     * 
     * <p>Usage with geometryInsertExpression():</p>
     * <pre>
     * String sql = "INSERT INTO table (geom) VALUES (" + 
     *              GeometrySqlHelper.geometryInsertExpression(dbType) + ")";
     * PreparedStatement ps = conn.prepareStatement(sql);
     * int nextIndex = GeometrySqlHelper.setGeometryParameter(ps, 1, myGeometry, dbType);
     * ps.setString(nextIndex, "value");
     * </pre>
     * 
     * @param ps Prepared statement
     * @param parameterIndex Index (1-based) of the parameter to set
     * @param geometry Geometry value (can be null)
     * @param dbType Database type (can be null, will be detected from connection)
     * @return Next available parameter index after the geometry parameter(s)
     * @throws SQLException If the geometry value cannot be set
     */
    public static int setGeometryParameter(PreparedStatement ps, int parameterIndex, Geometry geometry, DBTypes dbType) throws SQLException {
        if (geometry == null) {
            return setNullGeometryParameter(ps, parameterIndex);
        }
        
        // Use WKT format - works on both H2GIS and PostGIS with ST_GeomFromText
        ps.setString(parameterIndex++, WKT_WRITER.write(geometry));
        ps.setInt(parameterIndex++, extractSRID(geometry));
        return parameterIndex;
    }
    
    /**
     * Set null geometry parameter (WKT text + SRID).
     * Both parameters are set to NULL to match the ST_SetSRID(ST_GeomFromText(?, ?), ?) pattern.
     * 
     * @param ps Prepared statement
     * @param parameterIndex Starting parameter index
     * @return Next available parameter index
     * @throws SQLException If parameters cannot be set
     */
    private static int setNullGeometryParameter(PreparedStatement ps, int parameterIndex) throws SQLException {
        ps.setNull(parameterIndex++, Types.VARCHAR);  // WKT text
        ps.setNull(parameterIndex++, Types.INTEGER);  // SRID
        return parameterIndex;
    }
    
    /**
     * Extract SRID from geometry, falling back to geometry factory if needed.
     * 
     * @param geometry Geometry to extract SRID from
     * @return SRID value (may be 0 if not set)
     */
    private static int extractSRID(Geometry geometry) {
        int srid = geometry.getSRID();
        if (srid <= 0 && geometry.getFactory() != null) {
            srid = geometry.getFactory().getSRID();
        }
        return srid;
    }

    /**
     * Obtain a {@link SpatialResultSet} instance that is compatible with both H2GIS and PostgreSQL result sets.
     * <p>
    * When using H2GIS, the JDBC driver already supports {@code unwrap(SpatialResultSet.class)}. For PostgreSQL connections
    * wrapped with the H2GIS {@code postgis-jts} module, the driver exposes geometries directly but does not advertise
    * {@code SpatialResultSet}. In that case we dynamically create a proxy to expose the same API surface.
     * </p>
     *
     * @param rs The JDBC result set to adapt. The caller must not close {@code rs} separately; closing the returned wrapper
     *           will close the underlying result set.
     * @return A {@link SpatialResultSet} view over the provided result set.
     * @throws SQLException If {@code rs} is {@code null} or cannot be unwrapped and the wrapper cannot be created.
     */
    public static SpatialResultSet unwrapSpatialResultSet(ResultSet rs, DBTypes dbType) throws SQLException {
        if (rs == null) {
            throw new SQLException("ResultSet must not be null");
        }
        if (rs instanceof SpatialResultSet) {
            return (SpatialResultSet) rs;
        }
        try {
            return rs.unwrap(SpatialResultSet.class);
        } catch (SQLException unwrapException) {
            return createSpatialResultSetProxy(rs, dbType);
        }
    }

    private static SpatialResultSet createSpatialResultSetProxy(ResultSet rs, DBTypes dbType) {
        InvocationHandler handler = new InvocationHandler() {
            private Integer firstGeometryColumn = null;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                switch (name) {
                    case "getGeometry":
                        if (args == null || args.length == 0) {
                            return readGeometry(rs, getFirstGeometryColumn());
                        } else if (args.length == 1) {
                            if (args[0] instanceof Integer) {
                                int columnIndex = (Integer) args[0];
                                ResultSetMetaData meta = rs.getMetaData();
                                String columnLabel = meta.getColumnLabel(columnIndex);
                                return readGeometry(rs, columnLabel);
                            } else if (args[0] instanceof String) {
                                return readGeometry(rs, (String) args[0]);
                            }
                        }
                        break;
                    case "updateGeometry":
                        if (args != null && args.length == 2) {
                            if (args[0] instanceof Integer && args[1] instanceof Geometry) {
                                rs.updateObject((Integer) args[0], args[1]);
                                return null;
                            } else if (args[0] instanceof String && args[1] instanceof Geometry) {
                                rs.updateObject((String) args[0], args[1]);
                                return null;
                            }
                        }
                        break;
                    case "isWrapperFor":
                        if (args != null && args.length == 1 && args[0] instanceof Class) {
                            Class<?> iface = (Class<?>) args[0];
                            return iface.isAssignableFrom(SpatialResultSet.class) || rs.isWrapperFor(iface);
                        }
                        break;
                    case "unwrap":
                        if (args != null && args.length == 1 && args[0] instanceof Class) {
                            Class<?> iface = (Class<?>) args[0];
                            if (iface.isAssignableFrom(SpatialResultSet.class)) {
                                return proxy;
                            }
                            return rs.unwrap(iface);
                        }
                        break;
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return "SpatialResultSetProxy(" + rs + ")";
                    default:
                        break;
                }
                try {
                    return method.invoke(rs, args);
                } catch (InvocationTargetException ex) {
                    throw ex.getTargetException();
                }
            }

            private Geometry readGeometry(ResultSet delegate, String columnLabel) throws SQLException {
                return GeometrySqlHelper.getGeometry(delegate, columnLabel, dbType);
            }

            private Geometry readGeometry(ResultSet delegate, int columnIndex) throws SQLException {
                ResultSetMetaData meta = delegate.getMetaData();
                String columnLabel = meta.getColumnLabel(columnIndex);
                return readGeometry(delegate, columnLabel);
            }

            private int getFirstGeometryColumn() throws SQLException {
                if (firstGeometryColumn != null) {
                    return firstGeometryColumn;
                }
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    String typeName = meta.getColumnTypeName(columnIndex);
                    if (typeName != null && typeName.toLowerCase(Locale.ROOT).startsWith("geometry")) {
                        firstGeometryColumn = columnIndex;
                        return firstGeometryColumn;
                    }
                }
                throw new SQLException("ResultSet does not contain geometry column");
            }
        };

        return (SpatialResultSet) Proxy.newProxyInstance(
                SpatialResultSet.class.getClassLoader(),
                new Class<?>[]{SpatialResultSet.class},
                handler);
    }

    /**
     * Get SQL expression for inserting geometry values via prepared statements.
     * 
     * Returns ST_SetSRID(ST_GeomFromText(?), ?) for both H2GIS and PostGIS,
     * as both databases support this PostGIS-compatible syntax.
     * 
     * @param dbType Database type (unused, kept for API compatibility)
     * @return SQL expression "ST_SetSRID(ST_GeomFromText(?), ?)"
     */
    public static String geometryInsertExpression(DBTypes dbType) {
        // Both H2GIS and PostGIS support this syntax
        return "ST_SetSRID(ST_GeomFromText(?), ?)";
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
        
        // Check if it's a net.postgis.jdbc.PGgeometry (PostGIS JDBC extension)
        // Use reflection to avoid hard dependency on postgis-jdbc
        if (geomObj.getClass().getName().equals("net.postgis.jdbc.PGgeometry")) {
            try {
                // PGgeometry.getGeometry() returns net.postgis.jdbc.geometry.Geometry
                // This PostGIS geometry has a getJTSGeometry() method that converts to JTS
                java.lang.reflect.Method getGeometryMethod = geomObj.getClass().getMethod("getGeometry");
                Object postgisGeom = getGeometryMethod.invoke(geomObj);
                
                if (postgisGeom == null) {
                    return null;
                }
                
                // Try to use the getJTSGeometry() method if available (newer PostGIS JDBC versions)
                try {
                    java.lang.reflect.Method getJTSGeometryMethod = postgisGeom.getClass().getMethod("getJTSGeometry");
                    return (Geometry) getJTSGeometryMethod.invoke(postgisGeom);
                } catch (NoSuchMethodException nsme) {
                    // Fallback: manually convert WKT to JTS
                    // PostGIS geometry toString() returns "SRID=xxx;WKT" format
                    // We need to strip the SRID prefix
                    String wktWithSRID = postgisGeom.toString();
                    String wkt;
                    if (wktWithSRID.startsWith("SRID=")) {
                        int semicolonIndex = wktWithSRID.indexOf(';');
                        wkt = wktWithSRID.substring(semicolonIndex + 1);
                    } else {
                        wkt = wktWithSRID;
                    }
                    
                    return new WKTReader().read(wkt);
                }
            } catch (Exception e) {
                throw new SQLException("Failed to extract geometry from PGgeometry object in column " + columnName, e);
            }
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
