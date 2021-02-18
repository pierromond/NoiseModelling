/**
 * @Author Aumond Pierre, Université Gustave Eiffel
 */

package org.noise_planet.noisemodelling.wps.Import_and_Export

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import jdk.internal.org.xml.sax.SAXException
import org.apache.commons.io.FilenameUtils
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.TableUtilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.InputSource
import org.xml.sax.XMLReader
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.helpers.XMLReaderFactory

import java.nio.channels.FileChannel
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Statement
import org.xml.sax.Attributes


title = 'Import File'
description = 'Import file into a database table (csv, dbf, geojson, gpx, bz2, gz, osm, shp, tsv)'

inputs = [pathFile       : [name: 'Path of the input File', description: 'Path of the input File (including extension .csv, .shp, etc.)', title: 'Path of the input File', type: String.class],
          databaseName   : [name: 'Name of the database', title: 'Name of the database', description: 'Name of the database (default : first found db)', min: 0, max: 1, type: String.class],
          tini           : [name: 'T_ini', title: 'T_ini', description: 'Define the initial time step (default = 1)', min: 0, max: 1, type: String.class],
          tend           : [name: 'T_end', title: 'T_end', description: 'Define the final time step (default = 100)', min: 0, max: 1, type: String.class],
          defaultSRID    : [name: 'Default SRID', title: 'Default SRID', description: 'If the layer does not include SRID properties, it will take this value (default : 4326)', min: 0, max: 1, type: Integer.class],
          outputTableName: [name: 'outputTableName', description: 'Do not write the name of a table that contains a space. (default : file name without extension)', title: 'Name of output table', min: 0, max: 1, type: String.class]]

outputs = [tableNameCreated: [name: 'tableNameCreated', title: 'tableNameCreated', type: String.class]]

static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}


def run(input) {

    // Get name of the database
    String dbName = ""
    if (input['databaseName']) {
        dbName = input['databaseName'] as String
    }

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable { Connection connection ->
        exec(connection, input)
    }
}


def exec(connection, input) {

    // output string, the information given back to the user
    String resultString = null

    // Create a logger to display messages in the geoserver logs and in the command prompt.
    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

    // print to command window
    logger.info('Start : Import Symuvia File')
    logger.info("inputs {}", input) // log inputs of the run

    // Create a second sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    Integer defaultSRID = 4326
    if (input['defaultSRID']) {
        defaultSRID = input['defaultSRID'] as Integer
    }

    int t_end = 100
    if (input['tend']) {
        t_end = Integer.valueOf(input['tend'])
    }

    int t_ini = 1
    if (input['tini']) {
        t_ini = Integer.valueOf(input['tini'])
    }


    String pathFile = input["pathFile"] as String
    String fileName = FilenameUtils.removeExtension(new File(pathFile).getName())

    String outputTableName = input["outputTableName"] as String
    if (!outputTableName) {
        outputTableName = fileName
    }
    outputTableName = outputTableName.toUpperCase()

    sql.execute("drop table "+outputTableName+"_TRAJ if exists ")
    sql.execute("drop table "+outputTableName+"_INST if exists ")

    Statement stmt = connection.createStatement()
    String dropOutputTable = "drop table if exists " + outputTableName
    stmt.execute(dropOutputTable)

    String ext = pathFile.substring(pathFile.lastIndexOf('.') + 1, pathFile.length())
    switch (ext) {
        case "xml":
            SYMUVIADriverFunction symuviaDriver = new SYMUVIADriverFunction()
            symuviaDriver.importFile(connection, outputTableName, new File(pathFile), new EmptyProgressVisitor())
            break
    }

    sql.execute("drop table temp2 if exists ;")
    sql.execute("CREATE TABLE temp2 (pk int AUTO_INCREMENT PRIMARY KEY, the_geom geometry, ID_VEH int, SPEED double, \n"+
            "ACC double, T int, veh_type char) as select NULL, ST_SETSRID(ST_MakePoint(traj.ABS, traj.ORD), 2154), traj.ID, traj.vit, traj.acc,\n"+
            "traj.INST, traj.TYPE from "+outputTableName+"_TRAJ traj where traj.INST >="+t_ini+" AND traj.INST <="+t_end+"  ;")

    sql.execute("drop table "+outputTableName+"_TRAJ_"+t_ini+"_"+t_end+" if exists ")
    sql.execute("create table "+outputTableName+"_TRAJ_"+t_ini+"_"+t_end+" as select * from temp2")
    sql.execute("drop table temp2")
    sql.execute("drop table "+outputTableName+"_INST if exists ")


    /* int srid = SFSUtilities.getSRID(connection, TableLocation.parse(outputTableName+"_TRAJ"))
     if(srid == 0) {
         connection.createStatement().execute(String.format("UPDATE %s SET THE_GEOM = ST_SetSRID(the_geom,%d)",
                 TableLocation.parse(outputTableName+"_TRAJ").toString(), defaultSRID))

     }*/


    def file = new File(pathFile)
    String returnString = null

    if (file.exists()) {
        returnString = "The table "+outputTableName +"_TRAJ_"+t_ini+"_"+t_end+" has been uploaded to database!"
    } else {
        returnString = "The input file is not found"
    }


    return [tableNameCreated: returnString]


}


class SYMUVIADriverFunction {
    public static String DESCRIPTION = "SYMUVIA file (ver. x.x)";

    void importFile(Connection connection, String tableReference, File fileName, ProgressVisitor progress) throws SQLException, IOException {
        importFile(connection, tableReference, fileName, progress, false)
    }

    /**
     *
     * @param connection Active connection, do not close this connection.
     * @param tableReference prefix uses to store the SYMUVIA tables
     * @param fileName File path to read
     * @param progress
     * @param deleteTables true to delete the existing tables
     * @throws SQLException Table write error
     * @throws IOException File read error
     */
    void importFile(Connection connection, String tableReference, File fileName, ProgressVisitor progress, boolean deleteTables) throws SQLException, IOException {
        if (fileName == null || !(fileName.getName().endsWith(".xml"))) {
            throw new IOException(new IllegalArgumentException("This driver handle only .xml files"))
        }
        if (deleteTables) {
            SYMUVIATablesFactory.dropSYMUVIATables(connection, JDBCUtilities.isH2DataBase(connection.getMetaData()), tableReference)
        }
        SYMUVIAParser symuviap = new SYMUVIAParser()
        symuviap.read(connection, tableReference, fileName, progress)
    }


    class InstSYMUVIAElement {

        private double val

        /**
         * Constructor
         * @param val Latitude value
         */
        InstSYMUVIAElement(double val) {
            this.val = val
        }


        /**
         * The val of the element
         *
         * @return
         */
        double getVAL() {
            return val
        }


    }

    class SYMUVIAParser extends DefaultHandler {

        private static final int BATCH_SIZE = 100;
        private PreparedStatement instPreparedStmt;
        private PreparedStatement trajPreparedStmt;

        private int instPreparedStmtBatchSize = 0;
        private int trajPreparedStmtBatchSize = 0;


        private InstSYMUVIAElement instSYMUVIAElement;
        private TrajSYMUVIAElement trajSYMUVIAElement;

        private ProgressVisitor progress = new EmptyProgressVisitor();
        private FileChannel fc;
        private long fileSize = 0;
        private long readFileSizeEachNode = 1;
        private long nodeCountProgress = 0;
        // For progression information return
        private static final int AVERAGE_NODE_SIZE = 500;
        private double indice_val = 0;

        public SYMUVIAParser() {

        }

        /**
         * Read the SYMUVIA file and create its corresponding tables.
         *
         * @param inputFile
         * @param tableName
         * @param connection
         * @param progress
         * @return
         * @throws SQLException
         */
        public boolean read(Connection connection, String tableName, File inputFile, ProgressVisitor progress) throws SQLException {
            this.progress = progress.subProcess(100);
            // Initialisation
            final boolean isH2 = JDBCUtilities.isH2DataBase(connection.getMetaData());
            boolean success = false;
            TableLocation requestedTable = TableLocation.parse(tableName, isH2);
            String symuviaTableName = requestedTable.getTable();
            checkSYMUVIATables(connection, isH2, requestedTable, symuviaTableName);
            createSYMUVIADatabaseModel(connection, isH2, requestedTable, symuviaTableName);

            FileInputStream fs = null;
            try {
                fs = new FileInputStream(inputFile);
                this.fc = fs.getChannel();
                this.fileSize = fc.size();
                // Given the file size and an average node file size.
                // Skip how many nodes in order to update progression at a step of 1%
                readFileSizeEachNode = Math.max((long) 1, (long) ((this.fileSize / AVERAGE_NODE_SIZE) / 100))
                nodeCountProgress = 0;
                XMLReader parser = XMLReaderFactory.createXMLReader();
                parser.setErrorHandler(this);
                parser.setContentHandler(this);
                if (inputFile.getName().endsWith(".xml")) {
                    parser.parse(new InputSource(fs));
                } else {
                    throw new SQLException("Supported formats are .xml");
                }
                success = true;
            } catch (SAXException ex) {
                throw new SQLException(ex);
            } catch (IOException ex) {
                throw new SQLException("Cannot parse the file " + inputFile.getAbsolutePath(), ex);
            } finally {
                try {
                    if (fs != null) {
                        fs.close();
                    }
                } catch (IOException ex) {
                    throw new SQLException("Cannot close the file " + inputFile.getAbsolutePath(), ex);
                }
                // When the reading ends, close() method has to be called
                if (instPreparedStmt != null) {
                    instPreparedStmt.close();
                }
                if (trajPreparedStmt != null) {
                    trajPreparedStmt.close();
                }

            }

            return success;
        }

        /**
         * Check if one table already exists
         *
         * @param connection
         * @param isH2
         * @param requestedTable
         * @param symuviaTableName
         * @throws SQLException
         */
        private void checkSYMUVIATables(Connection connection, boolean isH2, TableLocation requestedTable, String symuviaTableName) throws SQLException {
            String[] omsTables = [SYMUVIATablesFactory.INST, SYMUVIATablesFactory.TRAJ]
            for (String omsTableSuffix : omsTables) {
                String symuviaTable = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + omsTableSuffix, isH2);
                if (JDBCUtilities.tableExists(connection, symuviaTable)) {
                    throw new SQLException("The table " + symuviaTable + " already exists.");
                }
            }
        }

        /**
         * Create the OMS data model to store the content of the file
         *
         * @param connection
         * @param isH2
         * @param requestedTable
         * @param symuviaTableName
         * @throws SQLException
         */
        private void createSYMUVIADatabaseModel(Connection connection, boolean isH2, TableLocation requestedTable, String symuviaTableName) throws SQLException {
            String instTableName = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + SYMUVIATablesFactory.INST, isH2);
            instPreparedStmt = SYMUVIATablesFactory.createInstTable(connection, instTableName, isH2);
            String trajTableName = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + SYMUVIATablesFactory.TRAJ, isH2);
            trajPreparedStmt = SYMUVIATablesFactory.createTrajTable(connection, trajTableName, isH2);
        }


        @Override
        void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (progress.isCanceled()) {
                throw new SAXException("Canceled by user");
            }
            if (localName.compareToIgnoreCase("INST") == 0) {
                instSYMUVIAElement = new InstSYMUVIAElement(Double.valueOf(attributes.getValue("val")));

            } else if (localName.compareToIgnoreCase("TRAJ") == 0) {
                trajSYMUVIAElement = new TrajSYMUVIAElement(Double.valueOf(attributes.getValue("abs")), Double.valueOf(attributes.getValue("acc")), Double.valueOf(attributes.getValue("dst")), Long.valueOf(attributes.getValue("id")), Double.valueOf(attributes.getValue("ord")), String.valueOf(attributes.getValue("type")), Double.valueOf(attributes.getValue("vit")));
            }
        }

        @Override
        public void endDocument() throws SAXException {
            // Execute remaining batch
            try {
                instPreparedStmtBatchSize = insertBatch(instPreparedStmt, instPreparedStmtBatchSize, 1);
                trajPreparedStmtBatchSize = insertBatch(trajPreparedStmt, trajPreparedStmtBatchSize, 1);
            } catch (SQLException ex) {
                throw new SAXException("Could not insert sql batch", ex);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {

            if (localName.compareToIgnoreCase("INST") == 0) {
                try {

                    instPreparedStmt.setObject(1, instSYMUVIAElement.getVAL());
                    indice_val = instSYMUVIAElement.getVAL();
                    instPreparedStmt.addBatch();
                    instPreparedStmtBatchSize++;
                } catch (SQLException ex) {
                    throw new SAXException("Cannot insert the node  :  " + instSYMUVIAElement.getVAL(), ex);
                }
            } else if (localName.compareToIgnoreCase("TRAJ") == 0) {
                try {
                    trajPreparedStmt.setObject(1, indice_val);
                    trajPreparedStmt.setObject(2, trajSYMUVIAElement.getABS());
                    trajPreparedStmt.setObject(3, trajSYMUVIAElement.getACC());
                    trajPreparedStmt.setObject(4, trajSYMUVIAElement.getDST());
                    trajPreparedStmt.setObject(5, trajSYMUVIAElement.getID());
                    trajPreparedStmt.setObject(6, trajSYMUVIAElement.getORD());
                    trajPreparedStmt.setString(7, trajSYMUVIAElement.getType());
                    trajPreparedStmt.setObject(8, trajSYMUVIAElement.getVIT());
                    trajPreparedStmt.addBatch();
                    trajPreparedStmtBatchSize++;
                } catch (SQLException ex) {
                    throw new SAXException("Cannot insert the traj  :  " + trajSYMUVIAElement.getABS(), ex);
                }
            }
            try {
                insertBatch();
            } catch (SQLException ex) {
                throw new SAXException("Could not insert sql batch", ex);
            }
            if (nodeCountProgress++ % readFileSizeEachNode == 0) {
                // Update Progress
                try {
                    progress.setStep((int) (((double) fc.position() / fileSize) * 100));
                } catch (IOException ex) {
                    // Ignore
                }
            }
        }

        private void insertBatch() throws SQLException {
            instPreparedStmtBatchSize = insertBatch(instPreparedStmt, instPreparedStmtBatchSize);
            trajPreparedStmtBatchSize = insertBatch(trajPreparedStmt, trajPreparedStmtBatchSize);
        }

        private int insertBatch(PreparedStatement st, int batchSize, int maxBatchSize) throws SQLException {
            if (batchSize >= maxBatchSize) {
                st.executeBatch();
                return 0;
            } else {
                return batchSize;
            }
        }

        private int insertBatch(PreparedStatement st, int batchSize) throws SQLException {
            return insertBatch(st, batchSize, BATCH_SIZE);
        }


    }

    /**
     * A class to manage the traj element properties.
     *
     * @author Pierre Aumond
     */
    class TrajSYMUVIAElement {

        private double abs
        private double acc
        private long id
        private double dst
        private double ord
        private String type
        private double vit


        /**
         * Constructor
         * @param abs Latitude value
         * @param acc Longitude value
         * @param id Longitude value
         * @param dst Longitude value
         * @param ord Longitude value
         * @param type Longitude value
         * @param vit Longitude value
         */
        TrajSYMUVIAElement(double abs, double acc, double dst, long id, double ord, String type, double vit) {
            this.abs = abs
            this.acc = acc
            this.id = id
            this.dst = dst
            this.ord = ord
            this.type = type
            this.vit = vit
        }
        /**
         * The id of the element
         *
         * @return
         */
        long getID() {
            return id
        }

        /**
         * Set an id to the element
         *
         * @param id
         */

        double getABS() {
            return abs
        }

        double getACC() {
            return acc
        }

        double getDST() {
            return dst
        }

        double getORD() {
            return ord
        }

        double getVIT() {
            return vit
        }

        /**
         * Type
         *
         * @return
         */
        String getType() {
            return type
        }

        void setType(String type) {
            this.type = type
        }


    }


/**
 * Class to create the tables to import symuvia data
 *
 * An SYMUVIA file is stored in 1 table.
 *
 * (1) table_prefix + _all :  table that contains all
 *
 * @author Pierre Aumond
 */
    class SYMUVIATablesFactory {

        //Suffix table names
        public static final String TRAJ = "_traj"
        public static final String INST = "_inst"


        private SYMUVIATablesFactory() {

        }

        /**
         * Create the global table that will be used to import SYMUVIA nodes
         * @param connection
         * @param instTableName
         * @param isH2
         * @return
         * @throws SQLException
         */
        static PreparedStatement createInstTable(Connection connection, String instTableName, boolean isH2) throws SQLException {
            Statement stmt = connection.createStatement()
            StringBuilder sb = new StringBuilder("CREATE TABLE ")
            sb.append(instTableName)
            sb.append("(val DOUBLE PRECISION);")
            stmt.execute(sb.toString())
            stmt.close()
            return connection.prepareStatement("INSERT INTO " + instTableName + " VALUES (?);")
        }


        /**
         * Create the global table that will be used to import SYMUVIA nodes
         * @param connection
         * @param trajTableName
         * @param isH2
         * @return
         * @throws SQLException
         */
        static PreparedStatement createTrajTable(Connection connection, String trajTableName, boolean isH2) throws SQLException {
            Statement stmt = connection.createStatement()
            StringBuilder sb = new StringBuilder("CREATE TABLE ")
            sb.append(trajTableName)
            sb.append("(inst DOUBLE PRECISION,"
                    + "abs DOUBLE PRECISION,"
                    + "acc DOUBLE PRECISION,"
                    + "dst DOUBLE PRECISION,"
                    + "id INTEGER,"
                    + "ord DOUBLE PRECISION,"
                    + "type VARCHAR,"
                    + "vit DOUBLE PRECISION);")
            stmt.execute(sb.toString())
            stmt.close()
            return connection.prepareStatement("INSERT INTO " + trajTableName + " VALUES (?,?,?,?,?,?,?,?);")
        }


        /**
         * Drop the existing SYMUVIA tables used to store the imported SYMUVIA data
         *
         * @param connection
         * @param isH2
         * @param tablePrefix
         * @throws SQLException
         */
        static void dropSYMUVIATables(Connection connection, boolean isH2, String tablePrefix) throws SQLException {
            TableLocation requestedTable = TableLocation.parse(tablePrefix, isH2)
            String symuviaTableName = requestedTable.getTable()
            String[] omsTables = String[INST, TRAJ]
            StringBuilder sb = new StringBuilder("drop table if exists ")
            String omsTableSuffix = omsTables[0]
            String symuviaTable = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + omsTableSuffix, isH2)
            sb.append(symuviaTable)
            for (int i = 1; i < omsTables.length; i++) {
                omsTableSuffix = omsTables[i]
                symuviaTable = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + omsTableSuffix, isH2)
                sb.append(",").append(symuviaTable)
            }
            Statement stmt = connection.createStatement()
            stmt.execute(sb.toString())
            stmt.close()
        }
    }
}
