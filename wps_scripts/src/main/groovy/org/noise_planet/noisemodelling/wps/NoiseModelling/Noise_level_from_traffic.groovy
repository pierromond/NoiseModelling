/**
 * NoiseModelling is a free and open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by Université Gustave Eiffel and CNRS
 * <http://noise-planet.org/noisemodelling.html>
 * as part of:
 * the Eval-PDU project (ANR-08-VILL-0005) 2008-2011, funded by the Agence Nationale de la Recherche (French)
 * the CENSE project (ANR-16-CE22-0012) 2017-2021, funded by the Agence Nationale de la Recherche (French)
 * the Nature4cities (N4C) project, funded by European Union’s Horizon 2020 research and innovation programme under grant agreement No 730468
 *
 * Noisemap is distributed under GPL 3 license.
 *
 * Contact: contact@noise-planet.org
 *
 * Copyright (C) 2011-2012 IRSTV (FR CNRS 2488) and Ifsttar
 * Copyright (C) 2013-2019 Ifsttar and CNRS
 * Copyright (C) 2020 Université Gustave Eiffel and CNRS
 *
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */

package org.noise_planet.noisemodelling.wps.NoiseModelling

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.noise_planet.noisemodelling.emission.jdbc.LDENConfig
import org.noise_planet.noisemodelling.emission.jdbc.LDENPointNoiseMapFactory
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.noise_planet.noisemodelling.propagation.RootProgressVisitor
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

import java.sql.Connection
import java.sql.SQLException

title = 'Compute LDay,Levening,LNight,Lden from road traffic'
description = 'Compute Lday noise map from Day Evening Night traffic flow rate and speed estimates (specific format, see input details).' +
        '</br> Tables must be projected in a metric coordinate system (SRID). Use "Change_SRID" WPS Block if needed.' +
        '</br> </br> <b> The output tables are called : LDAY_GEOM LEVENING_GEOM LNIGHT_GEOM LDEN_GEOM </b> ' +
        'and contain : </br>' +
        '-  <b> IDRECEIVER  </b> : an identifier (INTEGER, PRIMARY KEY). </br>' +
        '- <b> THE_GEOM </b> : the 3D geometry of the receivers (POINT).</br> ' +
        '-  <b> Hz63, Hz125, Hz250, Hz500, Hz1000,Hz2000, Hz4000, Hz8000 </b> : 8 columns giving the day emission sound level for each octave band (FLOAT).'

inputs = [tableBuilding     : [name       : 'Buildings table name', title: 'Buildings table name',
                               description: '<b>Name of the Buildings table.</b>  </br>  ' +
                                       '<br>  The table shall contain : </br>' +
                                       '- <b> THE_GEOM </b> : the 2D geometry of the building (POLYGON or MULTIPOLYGON). </br>' +
                                       '- <b> HEIGHT </b> : the height of the building (FLOAT)',
                               type       : String.class],
          tableRoads: [name: 'Roads table name', title: 'Roads table name', description: "<b>Name of the Roads table.</b>  </br>  " +
                  "<br>  This function recognize the following columns (* mandatory) : </br><ul>" +
                  "<li><b> PK </b>* : an identifier. It shall be a primary key (INTEGER, PRIMARY KEY)</li>" +
                  "<li><b> LV_D </b><b>TV_E </b><b> TV_N </b> : Hourly average light vehicle count (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> MV_D </b><b>MV_E </b><b>MV_N </b> : Hourly average medium heavy vehicles, delivery vans > 3.5 tons,  buses, touring cars, etc. with two axles and twin tyre mounting on rear axle count (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> HGV_D </b><b> HGV_E </b><b> HGV_N </b> :  Hourly average heavy duty vehicles, touring cars, buses, with three or more axles (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> WAV_D </b><b> WAV_E </b><b> WAV_N </b> :  Hourly average mopeds, tricycles or quads &le; 50 cc count (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> WBV_D </b><b> WBV_E </b><b> WBV_N </b> :  Hourly average motorcycles, tricycles or quads > 50 cc count (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> LV_SPD_D </b><b> LV_SPD_E </b><b>LV_SPD_N </b> :  Hourly average light vehicle speed (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> MV_SPD_D </b><b> MV_SPD_E </b><b>MV_SPD_N </b> :  Hourly average medium heavy vehicles speed (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> HGV_SPD_D </b><b> HGV_SPD_E </b><b> HGV_SPD_N </b> :  Hourly average heavy duty vehicles speed (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> WAV_SPD_D </b><b> WAV_SPD_E </b><b> WAV_SPD_N </b> :  Hourly average mopeds, tricycles or quads &le; 50 cc speed (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> WBV_SPD_D </b><b> WBV_SPD_E </b><b> WBV_SPD_N </b> :  Hourly average motorcycles, tricycles or quads > 50 cc speed (6-18h)(18-22h)(22-6h) (DOUBLE)</li>" +
                  "<li><b> PVMT </b> :  CNOSSOS road pavement identifier (ex: NL05)(default NL08) (VARCHAR)</li>" +
                  "<li><b> TEMP_D </b><b> TEMP_E </b><b> TEMP_N </b> : Average day, evening, night temperature (default 20&#x2103;) (6-18h)(18-22h)(22-6h)(DOUBLE)</li>" +
                  "<li><b> TS_STUD </b> : A limited period Ts (in months) over the year where a average proportion pm of light vehicles are equipped with studded tyres (0-12) (DOUBLE)</li>" +
                  "<li><b> PM_STUD </b> : Average proportion of vehicles equipped with studded tyres during TS_STUD period (0-1) (DOUBLE)</li>" +
                  "<li><b> JUNC_DIST </b> : Distance to junction in meters (DOUBLE)</li>" +
                  "<li><b> JUNC_TYPE </b> : Type of junction (k=0 none, k = 1 for a crossing with traffic lights ; k = 2 for a roundabout) (INTEGER)</li>" +
                  "</ul></br><b> This table can be generated from the WPS Block 'OsmToInputData'. </b>.", type: String.class],
          tableReceivers    : [name       : 'Receivers table name', title: 'Receivers table name',
                               description: '<b>Name of the Receivers table.</b></br>  ' +
                                       '</br>  The table shall contain : </br> ' +
                                       '- <b> PK </b> : an identifier. It shall be a primary key (INTEGER, PRIMARY KEY). </br> ' +
                                       '- <b> THE_GEOM </b> : the 3D geometry of the sources (POINT, MULTIPOINT).</br> ' +
                                       '</br> </br> <b> This table can be generated from the WPS Blocks in the "Receivers" folder. </b>',
                               type       : String.class],
          tableDEM          : [name       : 'DEM table name', title: 'DEM table name',
                               description: '<b>Name of the Digital Elevation Model table.</b></br>  ' +
                                       '</br>The table shall contain : </br> ' +
                                       '- <b> THE_GEOM </b> : the 3D geometry of the sources (POINT, MULTIPOINT).</br> ' +
                                       '</br> </br> <b> This table can be generated from the WPS Block "Import_Asc_File". </b>',
                               min        : 0, max: 1, type: String.class],
          tableGroundAbs    : [name       : 'Ground absorption table name', title: 'Ground absorption table name',
                               description: '<b>Name of the surface/ground acoustic absorption table.</b></br>  ' +
                                       '</br>The table shall contain : </br> ' +
                                       '- <b> THE_GEOM </b> : the 2D geometry of the sources (POLYGON or MULTIPOLYGON).</br> ' +
                                       '- <b> G </b> : the acoustic absorption of a ground (FLOAT between 0 : very hard and 1 : very soft).</br> ',
                               min        : 0, max: 1, type: String.class],
          paramWallAlpha    : [name       : 'wallAlpha', title: 'Wall absorption coefficient',
                               description: 'Wall absorption coefficient (FLOAT between 0 : fully absorbent and strictly less than 1 : fully reflective)' +
                                       '</br> </br> <b> Default value : 0.1 </b> ',
                               min        : 0, max: 1, type: String.class],
          confReflOrder     : [name       : 'Order of reflexion', title: 'Order of reflexion',
                               description: 'Maximum number of reflections to be taken into account (INTEGER).' +
                                       '</br> </br> <b> Default value : 1 </b>',
                               min        : 0, max: 1, type: String.class],
          confMaxSrcDist    : [name       : 'Maximum source-receiver distance', title: 'Maximum source-receiver distance',
                               description: 'Maximum distance between source and receiver (FLOAT, in meters).' +
                                       '</br> </br> <b> Default value : 150 </b>',
                               min        : 0, max: 1, type: String.class],
          confMaxReflDist   : [name       : 'Maximum source-reflexion distance', title: 'Maximum source-reflexion distance',
                               description: 'Maximum reflection distance from the source (FLOAT, in meters).' +
                                       '</br> </br> <b> Default value : 50 </b>',
                               min        : 0, max: 1, type: String.class],
          confThreadNumber  : [name       : 'Thread number', title: 'Thread number',
                               description: 'Number of thread to use on the computer (INTEGER).' +
                                       '</br> To set this value, look at the number of cores you have.' +
                                       '</br> If it is set to 0, use the maximum number of cores available.' +
                                       '</br> </br> <b> Default value : 1 </b>',
                               min        : 0, max: 1, type: String.class],
          confDiffVertical  : [name       : 'Diffraction on vertical edges', title: 'Diffraction on vertical edges',
                               description: 'Compute or not the diffraction on vertical edges.' +
                                       '</br> </br> <b> Default value : false </b>',
                               min        : 0, max: 1, type: Boolean.class],
          confDiffHorizontal: [name       : 'Diffraction on horizontal edges', title: 'Diffraction on horizontal edges',
                               description: 'Compute or not the diffraction on horizontal edges.' +
                                       '</br> </br> <b> Default value : false </b>',
                               min        : 0, max: 1, type: Boolean.class],
          confSkipLday: [name       : 'Skip LDAY_GEOM table', title: 'Do not compute LDAY_GEOM table',
                               description: 'Skip the creation of this table.' +
                                       '</br> </br> <b> Default value : false </b>',
                               min        : 0, max: 1, type: Boolean.class],
          confSkipLevening: [name       : 'Skip LEVENING_GEOM table', title: 'Do not compute LEVENING_GEOM table',
                         description: 'Skip the creation of this table.' +
                                 '</br> </br> <b> Default value : false </b>',
                         min        : 0, max: 1, type: Boolean.class],
          confSkipLnight: [name       : 'Skip LNIGHT_GEOM table', title: 'Do not compute LNIGHT_GEOM table',
                         description: 'Skip the creation of this table.' +
                                 '</br> </br> <b> Default value : false </b>',
                         min        : 0, max: 1, type: Boolean.class],
          confSkipLden: [name       : 'Skip LDEN_GEOM table', title: 'Do not compute LDEN_GEOM table',
                         description: 'Skip the creation of this table.' +
                                 '</br> </br> <b> Default value : false </b>',
                         min        : 0, max: 1, type: Boolean.class],
          confExportSourceId: [name       : 'keep source id', title: 'Separate receiver level by source identifier',
                         description: 'Keep source identifier in output in order to get noise contribution of each noise source.' +
                                 '</br> </br> <b> Default value : false </b>',
                         min        : 0, max: 1, type: Boolean.class],
          confHumidity: [name       : 'Relative humidity', title: 'Relative humidity',
                         description: 'Humidity for noise propagation, default value is <b>70</b>',
                         min        : 0, max: 1, type: Double.class],
          confTemperature: [name       : 'Temperature', title: 'Air temperature',
                            description: 'Air temperature in degree celsius, default value is <b>15</b>',
                            min        : 0, max: 1, type: Double.class],
          confFavorableOccurrences  : [name: 'Probability of occurrences table', title: 'Probability of occurrences table',
                                       description: 'Table of probability of occurrences of favourable propagation conditions.' +
                                               'The north slice is the last array index not the first one<br/>' +
                                               'Slice width are 22.5&#176;: (16 slices)<br/><ul>' +
                                               '<li>The first column 22.5&#176; contain occurrences between 11.25 to 33.75 &#176;</li>' +
                                               '<li>The last column 360&#176; contains occurrences between 348.75&#176; to 360&#176; and 0 to 11.25&#176;</li></ul>Default value <b>0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5</b>',
                                       min: 0, max: 1, type: String.class]]

outputs = [result: [name: 'Result output string', title: 'Result output string', description: 'This type of result does not allow the blocks to be linked together.', type: String.class]]

// Open Connection to Geoserver
static Connection openGeoserverDataStoreConnection(String dbName) {
    if (dbName == null || dbName.isEmpty()) {
        dbName = new GeoServer().catalog.getStoreNames().get(0)
    }
    Store store = new GeoServer().catalog.getStore(dbName)
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}

// run the script
def run(input) {

    // Get name of the database
    // by default an embedded h2gis database is created
    // Advanced user can replace this database for a postGis or h2Gis server database.
    String dbName = "h2gisdb"

    // Open connection
    openGeoserverDataStoreConnection(dbName).withCloseable {
        Connection connection ->
            return [result: exec(connection, input)]
    }
}

def forgeCreateTable(Sql sql, String tableName, LDENConfig ldenConfig, String geomField, String tableReceiver, String tableResult) {
    StringBuilder sb = new StringBuilder("create table ");
    sb.append(tableName);
    if(!ldenConfig.mergeSources) {
        sb.append(" (IDRECEIVER bigint NOT NULL");
        sb.append(", IDSOURCE bigint NOT NULL");
    } else {
        sb.append(" (IDRECEIVER bigint NOT NULL");
    }
    sb.append(", THE_GEOM geometry")
    for (int idfreq = 0; idfreq < PropagationProcessPathData.DEFAULT_FREQ_LVL.size(); idfreq++) {
        sb.append(", HZ");
        sb.append(PropagationProcessPathData.DEFAULT_FREQ_LVL.get(idfreq));
        sb.append(" numeric(5, 2)");
    }
    sb.append(", LAEQ numeric(5, 2), LEQ numeric(5, 2) ) AS SELECT PK");
    if(!ldenConfig.mergeSources) {
        sb.append(", IDSOURCE");
    }
    sb.append(", ")
    sb.append(geomField)
    for (int idfreq = 0; idfreq < PropagationProcessPathData.DEFAULT_FREQ_LVL.size(); idfreq++) {
        sb.append(", HZ");
        sb.append(PropagationProcessPathData.DEFAULT_FREQ_LVL.get(idfreq));
    }
    sb.append(", LAEQ, LEQ FROM ")
    sb.append(tableReceiver)
    if(!ldenConfig.mergeSources) {
        // idsource can't be null so we can't left join
        sb.append(" a, ")
        sb.append(tableResult)
        sb.append(" b WHERE a.PK = b.IDRECEIVER")
    } else {
        sb.append(" a LEFT JOIN ")
        sb.append(tableResult)
        sb.append(" b ON a.PK = b.IDRECEIVER")
    }
    sql.execute(sb.toString())
    // apply pk
    System.out.println("Add primary key on " + tableName)
    if(!ldenConfig.mergeSources) {
        sql.execute("ALTER TABLE " + tableName + " ADD PRIMARY KEY(IDRECEIVER, IDSOURCE)")
    } else {
        sql.execute("ALTER TABLE " + tableName + " ADD PRIMARY KEY(IDRECEIVER)")
    }
}

// main function of the script
def exec(Connection connection, input) {
    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postGIS database
    connection = new ConnectionWrapper(connection)

    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)

    // output string, the information given back to the user
    String resultString = null

    // print to command window
    System.out.println('Start : LDAY from Traffic')
    def start = new Date()

    // -------------------
    // Get every inputs
    // -------------------

    String sources_table_name = input['tableRoads'] as String
    // do it case-insensitive
    sources_table_name = sources_table_name.toUpperCase()

    //Get the geometry field of the source table
    TableLocation sourceTableIdentifier = TableLocation.parse(sources_table_name)
    List<String> geomFields = SFSUtilities.getGeometryFields(connection, sourceTableIdentifier)
    if (geomFields.isEmpty()) {
        resultString = String.format("The table %s does not exists or does not contain a geometry field", sourceTableIdentifier)
        throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", sourceTableIdentifier))
    }

    //Get the primary key field of the source table
    int pkIndex = JDBCUtilities.getIntegerPrimaryKey(connection, sources_table_name)
    if (pkIndex < 1) {
        resultString = String.format("Source table %s does not contain a primary key", sourceTableIdentifier)
        throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", sourceTableIdentifier))
    }

    String receivers_table_name = input['tableReceivers']
    // do it case-insensitive
    receivers_table_name = receivers_table_name.toUpperCase()

    //Get the geometry field of the receiver table
    TableLocation receiverTableIdentifier = TableLocation.parse(receivers_table_name)
    List<String> geomFieldsRcv = SFSUtilities.getGeometryFields(connection, receiverTableIdentifier)
    if (geomFieldsRcv.isEmpty()) {
        resultString = String.format("The table %s does not exists or does not contain a geometry field", receiverTableIdentifier)
        throw new SQLException(String.format("The table %s does not exists or does not contain a geometry field", receiverTableIdentifier))
    }

    //Get the primary key field of the receiver table
    int pkIndexRecv = JDBCUtilities.getIntegerPrimaryKey(connection, receivers_table_name)
    if (pkIndexRecv < 1) {
        resultString = String.format("Source table %s does not contain a primary key", receiverTableIdentifier)
        throw new IllegalArgumentException(String.format("Source table %s does not contain a primary key", receiverTableIdentifier))
    }

    String building_table_name = input['tableBuilding']
    // do it case-insensitive
    building_table_name = building_table_name.toUpperCase()


    String dem_table_name = ""
    if (input['tableDEM']) {
        dem_table_name = input['tableDEM']
    }
    // do it case-insensitive
    dem_table_name = dem_table_name.toUpperCase()

    String ground_table_name = ""
    if (input['tableGroundAbs']) {
        ground_table_name = input['tableGroundAbs']
    }
    // do it case-insensitive
    ground_table_name = ground_table_name.toUpperCase()

    int reflexion_order = 0
    if (input['confReflOrder']) {
        reflexion_order = Integer.valueOf(input['confReflOrder'])
    }

    double max_src_dist = 150
    if (input['confMaxSrcDist']) {
        max_src_dist = Double.valueOf(input['confMaxSrcDist'])
    }

    double max_ref_dist = 50
    if (input['confMaxReflDist']) {
        max_ref_dist = Double.valueOf(input['confMaxReflDist'])
    }

    double wall_alpha = 0.1
    if (input['paramWallAlpha']) {
        wall_alpha = Double.valueOf(input['paramWallAlpha'])
    }

    int n_thread = 1
    if (input['confThreadNumber']) {
        n_thread = Integer.valueOf(input['confThreadNumber'])
    }

    boolean compute_vertical_diffraction = false
    if (input['confDiffVertical']) {
        compute_vertical_diffraction = input['confDiffVertical']
    }

    boolean compute_horizontal_diffraction = false
    if (input['confDiffHorizontal']) {
        compute_horizontal_diffraction = input['confDiffHorizontal']
    }

    boolean confSkipLday = false;
    if (input['confSkipLday']) {
        confSkipLday = input['confSkipLday']
    }

    boolean confSkipLevening = false;
    if (input['confSkipLevening']) {
        confSkipLevening = input['confSkipLevening']
    }

    boolean confSkipLnight = false;
    if (input['confSkipLnight']) {
        confSkipLnight = input['confSkipLnight']
    }

    boolean confSkipLden = false;
    if (input['confSkipLden']) {
        confSkipLden = input['confSkipLden']
    }

    boolean confExportSourceId = false;
    if (input['confExportSourceId']) {
        confExportSourceId = input['confExportSourceId']
    }

    // -------------------------
    // Initialize some variables
    // -------------------------

    // Set of already processed receivers
    Set<Long> receivers = new HashSet<>()
    // --------------------------------------------
    // Initialize NoiseModelling propagation part
    // --------------------------------------------

    PointNoiseMap pointNoiseMap = new PointNoiseMap(building_table_name, sources_table_name, receivers_table_name)

    LDENConfig ldenConfig = new LDENConfig(LDENConfig.INPUT_MODE.INPUT_MODE_TRAFFIC_FLOW)

    ldenConfig.setComputeLDay(!confSkipLday)
    ldenConfig.setComputeLEvening(!confSkipLevening)
    ldenConfig.setComputeLNight(!confSkipLnight)
    ldenConfig.setComputeLDEN(!confSkipLden)
    ldenConfig.setMergeSources(!confExportSourceId)

    LDENPointNoiseMapFactory ldenProcessing = new LDENPointNoiseMapFactory(connection, ldenConfig)
    pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
    pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
    pointNoiseMap.setSoundReflectionOrder(reflexion_order)


    // Set environmental parameters
    PropagationProcessPathData environmentalData = new PropagationProcessPathData()

    if(input.containsKey('confHumidity')) {
        environmentalData.setHumidity(input['confHumidity'] as Double)
    }
    if(input.containsKey('confTemperature')) {
        environmentalData.setTemperature(input['confTemperature'] as Double)
    }
    if(input.containsKey('confFavorableOccurrences')) {
        StringTokenizer tk = new StringTokenizer(input['confFavorableOccurrences'] as String, ',')
        double[] favOccurrences = new double[PropagationProcessPathData.DEFAULT_WIND_ROSE.length]
        for(int i = 0; i < favOccurrences.length; i++) {
            favOccurrences[i] = Math.max(0, Math.min(1, Double.valueOf(tk.nextToken().trim())))
        }
        environmentalData.setWindRose(favOccurrences)
    }

    pointNoiseMap.setPropagationProcessPathData(environmentalData)

    // Building height field name
    pointNoiseMap.setHeightField("HEIGHT")
    // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
    if (ground_table_name != "") {
        pointNoiseMap.setSoilTableName(ground_table_name)
    }
    // Point cloud height above sea level POINT(X Y Z)
    if (dem_table_name != "") {
        pointNoiseMap.setDemTable(dem_table_name)
    }

    pointNoiseMap.setMaximumPropagationDistance(max_src_dist)
    pointNoiseMap.setMaximumReflectionDistance(max_ref_dist)
    pointNoiseMap.setWallAbsorption(wall_alpha)
    pointNoiseMap.setThreadCount(n_thread)

    // Do not propagate for low emission or far away sources
    // Maximum error in dB
    pointNoiseMap.setMaximumError(0.1d)

    // --------------------------------------------
    // Initialize NoiseModelling emission part
    // --------------------------------------------
    pointNoiseMap.setComputeRaysOutFactory(ldenProcessing)
    pointNoiseMap.setPropagationProcessDataFactory(ldenProcessing)

    // Init Map
    pointNoiseMap.initialize(connection, new EmptyProgressVisitor())

    // --------------------------------------------
    // Run Calculations
    // --------------------------------------------

    // Init ProgressLogger (loading bar)
    RootProgressVisitor progressLogger = new RootProgressVisitor(1, true, 1)
    ProgressVisitor progressVisitor = progressLogger.subProcess(pointNoiseMap.getGridDim() * pointNoiseMap.getGridDim())
    int fullGridSize = pointNoiseMap.getGridDim() * pointNoiseMap.getGridDim()

    System.println("Start calculation... ")

    try {
        ldenProcessing.start()
        // Iterate over computation areas
        int k = 0
        for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
            for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                System.println("Compute... " + 100 * k++ / fullGridSize + " % ")
                // Run ray propagation
                pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers)
            }
        }
    } finally {
        ldenProcessing.stop()
    }

    System.out.println('Intermediate  time : ' + TimeCategory.minus(new Date(), start))

    // Associate Geometry column to the table LDEN
    StringBuilder createdTables = new StringBuilder()

    if(ldenConfig.computeLDay) {
        sql.execute("drop table if exists LDAY_GEOM;")
        System.out.println('create table LDAY_GEOM')
        forgeCreateTable(sql, "LDAY_GEOM", ldenConfig, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig.lDayTable)
        createdTables.append(" LDAY_GEOM")
        sql.execute("drop table if exists "+TableLocation.parse(ldenConfig.getlDayTable()))
    }
    if(ldenConfig.computeLEvening) {
        sql.execute("drop table if exists LEVENING_GEOM;")
        System.out.println('create table LEVENING_GEOM')
        forgeCreateTable(sql, "LEVENING_GEOM", ldenConfig, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig.lEveningTable)
        createdTables.append(" LEVENING_GEOM")
        sql.execute("drop table if exists "+TableLocation.parse(ldenConfig.getlEveningTable()))
    }
    if(ldenConfig.computeLNight) {
        sql.execute("drop table if exists LNIGHT_GEOM;")
        System.out.println('create table LNIGHT_GEOM')
        forgeCreateTable(sql, "LNIGHT_GEOM", ldenConfig, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig.lNightTable)
        createdTables.append(" LNIGHT_GEOM")
        sql.execute("drop table if exists "+TableLocation.parse(ldenConfig.getlNightTable()))
    }
    if(ldenConfig.computeLDEN) {
        sql.execute("drop table if exists LDEN_GEOM;")
        System.out.println('create table LDEN_GEOM')
        forgeCreateTable(sql, "LDEN_GEOM", ldenConfig, geomFieldsRcv.get(0), receivers_table_name,
                ldenConfig.lDenTable)
        createdTables.append(" LDEN_GEOM")
        sql.execute("drop table if exists "+TableLocation.parse(ldenConfig.getlDenTable()))
    }

    resultString = "Calculation Done ! "+createdTables.toString()+" table(s) have been created."

    // print to command window
    System.out.println('Result : ' + resultString)
    System.out.println('End : LDAY from Traffic')
    System.out.println('Duration : ' + TimeCategory.minus(new Date(), start))

    // print to WPS Builder
    return resultString


}

