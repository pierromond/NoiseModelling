/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas.
 * It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in
 * Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE
 * provided with this software.
 *
 * Contact: contact@noise-planet.org
 */

package org.noise_planet.noisemodelling.scripts.TestSupport

import groovy.sql.Sql
import org.h2gis.functions.factory.H2GISDBFactory
import org.noise_planet.noisemodelling.jdbc.utils.PostgisConnectionWrapper
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.sql.SQLException

/**
 * Helper class for database testing with support for:
 * - H2GIS (default - fast, in-memory)
 * - PostgreSQL via Testcontainers (Docker-based, automatic PostGIS setup)
 * - External PostgreSQL (manual setup required)
 * 
 * Usage:
 * 1. H2GIS (default): mvn test
 * 2. Testcontainers: TEST_USE_POSTGIS=true TEST_USE_TESTCONTAINERS=true mvn test
 * 3. External PG: TEST_USE_POSTGIS=true mvn test
 */
class DatabaseTestHelper {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseTestHelper.class)
    
    // Testcontainers PostgreSQL instance (shared across all tests)
    private static PostgreSQLContainer<?> postgreSQLContainer
    private static boolean containerStarted = false
    
    // Environment variables
    private static final boolean USE_POSTGIS = 
        System.getenv("TEST_USE_POSTGIS")?.equalsIgnoreCase("true") ?: false
    private static final boolean USE_TESTCONTAINERS = 
        System.getenv("TEST_USE_TESTCONTAINERS")?.equalsIgnoreCase("true") ?: false
    
    /**
     * Create a database connection for testing
     * @param identifier Test identifier (used for H2GIS database name or PostgreSQL schema)
     * @return Connection to either H2GIS or PostgreSQL
     */
    static Connection open(String identifier = null) {
        if (USE_POSTGIS && USE_TESTCONTAINERS) {
            return openPostGISWithTestcontainers(identifier)
        } else if (USE_POSTGIS) {
            return openExternalPostGIS(identifier)
        } else {
            return openH2GIS(identifier)
        }
    }
    
    /**
     * Create H2GIS connection (default)
     */
    private static Connection openH2GIS(String identifier) {
        String dbName = identifier ? 
            "testdb_${identifier.replaceAll('[^A-Za-z0-9]', '_')}_${UUID.randomUUID().toString().substring(0, 8)}" : 
            "testdb_${UUID.randomUUID().toString().substring(0, 8)}"
        Connection connection = H2GISDBFactory.createSpatialDataBase(dbName, true)
        logger.info("✓ Using H2GIS database: ${dbName}")
        return connection
    }
    
    /**
     * Create PostgreSQL connection via Testcontainers
     */
    private static Connection openPostGISWithTestcontainers(String identifier) {
        // Start container if not already started
        if (!containerStarted) {
            synchronized (DatabaseTestHelper.class) {
                if (!containerStarted) {
                    startPostGISContainer()
                }
            }
        }
        
        // Connect to PostgreSQL and wrap with H2GIS PostGIS wrapper for JTS geometry support
        Connection rawConnection = postgreSQLContainer.createConnection("")
        Connection connection = new PostgisConnectionWrapper(rawConnection)
        
        // Create unique schema for this test
        String schemaName = identifier ? 
            "test_${identifier.replaceAll('[^A-Za-z0-9]', '_')}_${UUID.randomUUID().toString().substring(0, 8)}".toLowerCase() :
            "test_${UUID.randomUUID().toString().substring(0, 8)}".toLowerCase()
        
        Sql sql = new Sql(connection)
        sql.execute("CREATE SCHEMA IF NOT EXISTS ${schemaName}")
        sql.execute("SET search_path TO ${schemaName}, public")
        
        logger.info("✓ Using PostGIS container (schema: ${schemaName}), JDBC: ${postgreSQLContainer.getJdbcUrl()}")
        
        return connection
    }
    
    /**
     * Start PostgreSQL container with PostGIS
     */
    private static void startPostGISContainer() {
        try {
            logger.info("⏳ Starting PostGIS container (first run may take a minute to download image)...")
            
            postgreSQLContainer = new PostgreSQLContainer<>(
                DockerImageName.parse("postgis/postgis:15-3.3")
                    .asCompatibleSubstituteFor("postgres")
            )
            .withDatabaseName("noisemodelling")
            .withUsername("noisemodelling")
            .withPassword("noisemodelling")
            .withReuse(true)  // Reuse container across test runs for performance
            
            postgreSQLContainer.start()
            
            // Initialize PostGIS extension
            Connection conn = postgreSQLContainer.createConnection("")
            Sql sql = new Sql(conn)
            sql.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            sql.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology")
            conn.close()
            
            containerStarted = true
            logger.info("✓ PostGIS container started: ${postgreSQLContainer.getJdbcUrl()}")
            
        } catch (Exception e) {
            logger.error("❌ Failed to start PostGIS container. Make sure Docker is running.", e)
            logger.info("💡 Falling back to H2GIS...")
            containerStarted = false
            throw new RuntimeException("Failed to start PostGIS container. Docker may not be available.", e)
        }
    }
    
    /**
     * Create connection to external PostgreSQL server
     */
    private static Connection openExternalPostGIS(String identifier) {
        String host = System.getenv("TEST_POSTGIS_HOST") ?: "localhost"
        String port = System.getenv("TEST_POSTGIS_PORT") ?: "5432"
        String database = System.getenv("TEST_POSTGIS_DB") ?: "noisemodelling"
        String user = System.getenv("TEST_POSTGIS_USER") ?: "noisemodelling"
        String password = System.getenv("TEST_POSTGIS_PASSWORD") ?: "noisemodelling"
        
        String jdbcUrl = "jdbc:postgresql://${host}:${port}/${database}"
        
        try {
            Class.forName("org.postgresql.Driver")
            Connection rawConnection = java.sql.DriverManager.getConnection(jdbcUrl, user, password)
            
            // Wrap with H2GIS PostGIS wrapper for JTS geometry support
            Connection connection = new PostgisConnectionWrapper(rawConnection)
            
            // Create unique schema
            String schemaName = identifier ? 
                "test_${identifier.replaceAll('[^A-Za-z0-9]', '_')}_${UUID.randomUUID().toString().substring(0, 8)}".toLowerCase() :
                "test_${UUID.randomUUID().toString().substring(0, 8)}".toLowerCase()
            
            Sql sql = new Sql(connection)
            sql.execute("CREATE SCHEMA IF NOT EXISTS ${schemaName}")
            sql.execute("SET search_path TO ${schemaName}, public")
            
            logger.info("✓ Using external PostgreSQL (schema: ${schemaName}), JDBC: ${jdbcUrl}")
            
            return connection
        } catch (Exception e) {
            logger.error("❌ Failed to connect to external PostgreSQL at ${jdbcUrl}", e)
            throw new RuntimeException("Cannot connect to external PostgreSQL. Make sure the server is running and credentials are correct.", e)
        }
    }
    
    /**
     * Clean up database resources
     */
    static void cleanup(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                // For PostgreSQL, drop the schema
                if (USE_POSTGIS) {
                    Sql sql = new Sql(connection)
                    String currentSchema = sql.firstRow("SELECT current_schema() as schema")?.schema
                    if (currentSchema && currentSchema != "public") {
                        sql.execute("DROP SCHEMA IF EXISTS ${currentSchema} CASCADE")
                        logger.debug("✓ Dropped schema: ${currentSchema}")
                    }
                }
            } catch (SQLException e) {
                logger.warn("Failed to cleanup database: ${e.message}")
            }
        }
    }
    
    /**
     * Stop PostgreSQL container (called at JVM shutdown)
     */
    static void stopContainer() {
        if (postgreSQLContainer != null && containerStarted) {
            try {
                postgreSQLContainer.stop()
                logger.info("✓ PostGIS container stopped")
            } catch (Exception e) {
                logger.warn("Failed to stop PostGIS container: ${e.message}")
            }
        }
    }
    
    // Static initializer to register shutdown hook
    static {
        Runtime.getRuntime().addShutdownHook(new Thread({
            DatabaseTestHelper.stopContainer()
        }))
    }
}
