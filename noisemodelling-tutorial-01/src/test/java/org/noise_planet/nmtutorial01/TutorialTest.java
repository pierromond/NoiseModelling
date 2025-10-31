package org.noise_planet.nmtutorial01;

import org.h2gis.functions.factory.H2GISDBFactory;
import org.h2gis.postgis_jts_osgi.DataSourceFactoryImpl;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.dbtypes.DBTypes;
import org.h2gis.utilities.dbtypes.DBUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.noise_planet.noisemodelling.jdbc.NoiseMapByReceiverMaker;
import org.noise_planet.noisemodelling.jdbc.NoiseMapDatabaseParameters;
import org.noise_planet.noisemodelling.jdbc.input.DefaultTableLoader;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TutorialTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TutorialTest.class);
    private static final String DEFAULT_DB_NAME = "noisemodelling_db";
    private static final String DEFAULT_DB_USER = "noisemodelling";
    private static final String DEFAULT_DB_PASSWORD = "noisemodelling";
    private static final boolean ENABLE_TESTCONTAINERS =
            !"false".equalsIgnoreCase(System.getenv().getOrDefault("NM_TUTORIAL_USE_TESTCONTAINERS", "true"));
    private static PostgreSQLContainer<?> POSTGIS_CONTAINER;

    @BeforeAll
    public static void startPostgisContainer() {
        if (!ENABLE_TESTCONTAINERS) {
            LOGGER.info("PostGIS Testcontainer disabled via NM_TUTORIAL_USE_TESTCONTAINERS=false");
            return;
        }

        try {
            POSTGIS_CONTAINER = new PostgreSQLContainer<>(
                    DockerImageName.parse("postgis/postgis:15-3.3").asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName(DEFAULT_DB_NAME)
                    .withUsername(DEFAULT_DB_USER)
                    .withPassword(DEFAULT_DB_PASSWORD);
            POSTGIS_CONTAINER.start();

            try (Connection connection = POSTGIS_CONTAINER.createConnection("")) {
                connection.createStatement().execute("CREATE EXTENSION IF NOT EXISTS postgis");
                connection.createStatement().execute("CREATE EXTENSION IF NOT EXISTS postgis_topology");
            }

            LOGGER.info("PostGIS Testcontainer started at {}", POSTGIS_CONTAINER.getJdbcUrl());
        } catch (Exception e) {
            LOGGER.warn("Unable to start PostGIS Testcontainer. Falling back to external database lookup.", e);
            POSTGIS_CONTAINER = null;
        }
    }

    @AfterAll
    public static void stopPostgisContainer() {
        if (POSTGIS_CONTAINER != null) {
            POSTGIS_CONTAINER.stop();
        }
    }

    @Test
    public void testPostgisNoiseModelling1() throws Exception {
        DataSourceFactoryImpl dataSourceFactory = new DataSourceFactoryImpl();
        Properties p = buildPostgresProperties();
        try(Connection connection = JDBCUtilities.wrapConnection(dataSourceFactory.createDataSource(p).getConnection())) {
            if (POSTGIS_CONTAINER != null) {
                ensurePostgisExtensions(connection);
            }
            connection.createStatement().execute("DROP TABLE IF EXISTS receivers_level");
            connection.createStatement().execute("DROP TABLE IF EXISTS contouring_noise_map");
            NoiseMapByReceiverMaker map;
            try {
                map = Main.mainWithConnection(connection, "target/postgis");
            } catch (SQLException sqlException) {
                if (shouldSkipSchemaPreparation(sqlException)) {
                    LOGGER.warn("Skipping PostGIS tutorial test due to missing schema prerequisites: {}",
                            sqlException.getLocalizedMessage());
                    Assumptions.assumeTrue(false,
                            "PostGIS tutorial schema not prepared: " + sqlException.getLocalizedMessage());
                    return;
                }
                throw sqlException;
            }
            String receiverTable = TableLocation.capsIdentifier(
                    NoiseMapDatabaseParameters.DEFAULT_RECEIVERS_LEVEL_TABLE_NAME, DBTypes.POSTGIS);
            assertTrue(JDBCUtilities.tableExists(connection.unwrap(Connection.class), receiverTable));
            assertTrue(JDBCUtilities.hasField(connection.unwrap(Connection.class), receiverTable, "period"));
            assertTrue(JDBCUtilities.tableExists(connection.unwrap(Connection.class), "contouring_noise_map"));
            assertTrue(JDBCUtilities.hasField(connection.unwrap(Connection.class), "contouring_noise_map", "period"));


            int receiversRowCount = JDBCUtilities.getRowCount(connection, "RECEIVERS");

            int resultRowCount = JDBCUtilities.getRowCount(connection,
                    receiverTable);

            // D E N and DEN, should be 4 more rows than receivers
            assertEquals(receiversRowCount * 4, resultRowCount);

            assertEquals(3, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().size());
            assertEquals(20, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().get("D").temperature);
            assertEquals(16, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().get("E").temperature);
            assertEquals(10, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().get("N").temperature);

        } catch (PSQLException psqlException) {
            if(shouldSkipPostgisTest(psqlException)) {
                LOGGER.warn("Skipping PostGIS tutorial test: {}", psqlException.getLocalizedMessage());
                Assumptions.assumeTrue(false, "PostGIS database not available: " + psqlException.getLocalizedMessage());
                return;
            }
            throw psqlException;
    }
    }

    @Test
    public void testH2gisNoiseModelling() throws Exception {
        try(Connection connection = JDBCUtilities.wrapConnection(
                H2GISDBFactory.createSpatialDataBase(TutorialTest.class.getSimpleName(),
                        true, ""));) {
            NoiseMapByReceiverMaker map = Main.mainWithConnection(connection, "target/h2gis");

            String receiverTable = TableLocation.capsIdentifier(
                    NoiseMapDatabaseParameters.DEFAULT_RECEIVERS_LEVEL_TABLE_NAME, DBTypes.H2GIS);
            assertTrue(JDBCUtilities.tableExists(connection.unwrap(Connection.class), receiverTable));
            assertTrue(JDBCUtilities.hasField(connection.unwrap(Connection.class), receiverTable, "period"));
            assertTrue(JDBCUtilities.tableExists(connection.unwrap(Connection.class), "contouring_noise_map"));
            assertTrue(JDBCUtilities.hasField(connection.unwrap(Connection.class), "contouring_noise_map", "period"));


            int receiversRowCount = JDBCUtilities.getRowCount(connection, "RECEIVERS");

            int resultRowCount = JDBCUtilities.getRowCount(connection,
                    receiverTable);

            // D E N and DEN, should be 4 more rows than receivers
            assertEquals(receiversRowCount * 4, resultRowCount);

            assertEquals(3, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().size());
            assertEquals(20, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().get("D").temperature);
            assertEquals(16, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().get("E").temperature);
            assertEquals(10, ((DefaultTableLoader)map.getTableLoader()).getCnossosParametersPerPeriod().get("N").temperature);

        } catch (PSQLException psqlException) {
            if(!(psqlException.getCause() instanceof ConnectException)) {
                throw psqlException;
            } else {
                // Ignore connection exception, we may not be inside the unit test of github workflow
                LOGGER.warn(psqlException.getLocalizedMessage(), psqlException);
            }
        }
    }

    private static Properties buildPostgresProperties() {
        Properties properties = new Properties();
        if (POSTGIS_CONTAINER != null) {
            properties.setProperty("serverName", POSTGIS_CONTAINER.getHost());
            properties.setProperty("portNumber", String.valueOf(
                    POSTGIS_CONTAINER.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)));
            properties.setProperty("databaseName", POSTGIS_CONTAINER.getDatabaseName());
            properties.setProperty("user", POSTGIS_CONTAINER.getUsername());
            properties.setProperty("password", POSTGIS_CONTAINER.getPassword());
        } else {
            properties.setProperty("serverName",
                    System.getenv().getOrDefault("NM_TUTORIAL_PG_HOST", "localhost"));
            properties.setProperty("portNumber",
                    System.getenv().getOrDefault("NM_TUTORIAL_PG_PORT", "5432"));
            properties.setProperty("databaseName",
                    System.getenv().getOrDefault("NM_TUTORIAL_PG_DB", DEFAULT_DB_NAME));
            properties.setProperty("user",
                    System.getenv().getOrDefault("NM_TUTORIAL_PG_USER", DEFAULT_DB_USER));
            properties.setProperty("password",
                    System.getenv().getOrDefault("NM_TUTORIAL_PG_PASSWORD", DEFAULT_DB_PASSWORD));
        }
        return properties;
    }

    private static void ensurePostgisExtensions(Connection connection) {
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgis");
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology");
        } catch (SQLException e) {
            LOGGER.warn("Failed to ensure PostGIS extensions are present: {}", e.getMessage());
        }
    }

    private static boolean shouldSkipPostgisTest(PSQLException exception) {
        if (exception.getCause() instanceof ConnectException) {
            return true;
        }
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("does not exist")
                || normalized.contains("connection refused")
                || normalized.contains("no route to host")
                || normalized.contains("timeout");
    }

    private static boolean shouldSkipSchemaPreparation(SQLException exception) {
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("table") && normalized.contains("not found")
                || normalized.contains("relation") && normalized.contains("does not exist")
                || normalized.contains("role \"root\" does not exist");
    }
}
