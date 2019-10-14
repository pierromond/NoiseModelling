/**
 * NoiseMap is a scientific computation plugin for OrbisGIS developed in order to
 * evaluate the noise impact on urban mobility plans. This model is
 * based on the French standard method NMPB2008. It includes traffic-to-noise
 * sources evaluation and sound propagation processing.
 *
 * This version is developed at French IRSTV Institute and at IFSTTAR
 * (http://www.ifsttar.fr/) as part of the Eval-PDU project, funded by the
 * French Agence Nationale de la Recherche (ANR) under contract ANR-08-VILL-0005-01.
 *
 * Noisemap is distributed under GPL 3 license. Its reference contact is Judicaël
 * Picaut <judicael.picaut@ifsttar.fr>. It is maintained by Nicolas Fortin
 * as part of the "Atelier SIG" team of the IRSTV Institute <http://www.irstv.fr/>.
 *
 * Copyright (C) 2011 IFSTTAR
 * Copyright (C) 2011-2012 IRSTV (FR CNRS 2488)
 *
 * Noisemap is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Noisemap is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Noisemap. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */


/*
 * @Author Hesry Quentin
 * @Author Pierre Aumond
 * @Author Nicolas Fortin
 */

import geoserver.GeoServer
import geoserver.catalog.Store

import org.h2gis.api.ProgressVisitor
import org.geotools.jdbc.JDBCDataStore
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.FastObstructionTest
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData

import javax.xml.stream.XMLStreamException
import org.cts.crs.CRSException

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.PreparedStatement
import groovy.sql.Sql

import org.h2gis.api.EmptyProgressVisitor
import org.noisemodellingwps.utilities.WpsConnectionWrapper
import org.h2gis.utilities.wrapper.*

import org.noise_planet.noisemodelling.propagation.*
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.h2gis.utilities.SpatialResultSet
import org.locationtech.jts.geom.Geometry


import java.sql.SQLException
import java.util.ArrayList
import java.util.List

title = 'Compute Lday Map'
description = 'Compute Lday Map from AADF estimates'

inputs = [
        resultPath: [name: 'Path for result files', title: 'Path for result files',  type: String.class],
        buildingTableName: [name: 'Buildings table name', title: 'Buildings table name',  type: String.class],
        sourcesTableName: [name: 'Sources table name', title: 'Sources table name', type: String.class],
        receiversTableName: [name: 'Receivers table name', title: 'Receivers table name', type: String.class],
        demTableName: [name: 'DEM table name', title: 'DEM table name',min : 0, max : 1, type: String.class],
        groundTableName: [name: 'Ground table name', title: 'Ground table name',min : 0, max : 1, type: String.class],
        reflexionOrder: [name: 'Number of reflexion', title: 'Number of reflexion', description : 'Maximum number of reflexion to consider (default = 0)', min : 0, max : 1, type: String.class],
        maxSrcDistance: [name: 'Max Source Distance', title: 'Max Source Distance', description : 'Maximum distance to consider a sound source (default = 100 m)',min : 0, max : 1, type: String.class],
        maxRefDistance: [name: 'Max Reflexion Distance', title: 'Max Reflexion Distance',description : 'Maximum distance to consider a reflexion (default = 50 m)',min : 0, max : 1, type: String.class],
        wallAlpha: [name: 'wallAlpha', title: 'Wall alpha',description : 'Wall abosrption (default = 0.1)',min : 0, max : 1, type: String.class],
        threadNumber: [name: 'Thread number', title: 'Thread number',description : 'Number of thread to use on the computer (default = 1)',min : 0, max : 1, type: String.class],
        computeVertical: [name: 'Compute vertical diffraction', title: 'Compute vertical diffraction',description : 'Compute or not the vertical diffraction (default = false)',min : 0, max : 1, type: Boolean.class],
        computeHorizontal: [name: 'Compute horizontal diffraction', title: 'Compute horizontal diffraction',description : 'Compute or not the horizontal diffraction (default = false)',min : 0, max : 1, type: Boolean.class]
]

outputs = [
        result: [name: 'result', title: 'Result', type: String.class]
]

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */

class TrafficPropagationProcessData extends PropagationProcessData {
    // Lden values
    protected List<double[]> wjSourcesD = new ArrayList<>();

    private String AAFD_FIELD_NAME = "AADF"; // Annual Average Daily Flow (AADF) estimates
    private String ROAD_CATEGORY_FIELD_NAME = "CLAS_ADM";
    def lv_hourly_distribution = [ 0.56, 0.3, 0.21, 0.26, 0.69, 1.8, 4.29, 7.56, 7.09, 5.5, 4.96, 5.04,
            5.8, 6.08, 6.23, 6.67, 7.84, 8.01, 7.12, 5.44, 3.45, 2.26, 1.72, 1.12];
    def hv_hourly_distribution = [1.01, 0.97, 1.06, 1.39, 2.05, 3.18, 4.77, 6.33, 6.72, 7.32, 7.37, 7.4,
            6.16, 6.22, 6.84, 6.74, 6.23, 4.88, 3.79, 3.05, 2.36, 1.76, 1.34, 1.07];
    private static final int LDAY_START_HOUR = 6;
    private static final int LDAY_STOP_HOUR = 18;
    private static final double HV_PERCENTAGE = 0.1;

    public TrafficPropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder);
    }


    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {
        super.addSource(pk, geom, rs);

        // Read average 24h traffic
        double tmja = rs.getDouble(AAFD_FIELD_NAME);

        //130 km/h 1:Autoroute
        //80 km/h  2:Nationale
        //50 km/h  3:Départementale
        //50 km/h  4:Voirie CUN
        //50 km/h  5:Inconnu
        //50 km/h  6:Privée
        //50 km/h  7:Communale
        int road_cat = rs.getInt(ROAD_CATEGORY_FIELD_NAME);

        int roadType;
        if(road_cat == 1) {
            roadType = 10;
        } else if(road_cat == 2) {
            roadType = 42;
        } else {
            roadType = 62;
        }
        double speed_lv = 50;
        if(road_cat == 1) {
            speed_lv = 120;
        } else if(road_cat == 2) {
            speed_lv = 80;
        }

        /**
         * Vehicles category Table 3 P.31 CNOSSOS_EU_JRC_REFERENCE_REPORT
         * lv : Passenger cars, delivery vans ≤ 3.5 tons, SUVs , MPVs including trailers and caravans
         * mv: Medium heavy vehicles, delivery vans > 3.5 tons,  buses, touring cars, etc. with two axles and twin tyre mounting on rear axle
         * hgv: Heavy duty vehicles, touring cars, buses, with three or more axles
         * wav:  mopeds, tricycles or quads ≤ 50 cc
         * wbv:  motorcycles, tricycles or quads > 50 cc
         * @param lv_speed Average light vehicle speed
         * @param mv_speed Average medium vehicle speed
         * @param hgv_speed Average heavy goods vehicle speed
         * @param wav_speed Average light 2 wheels vehicle speed
         * @param wbv_speed Average heavy 2 wheels vehicle speed
         * @param lvPerHour Average light vehicle per hour
         * @param mvPerHour Average heavy vehicle per hour
         * @param hgvPerHour Average heavy vehicle per hour
         * @param wavPerHour Average heavy vehicle per hour
         * @param wbvPerHour Average heavy vehicle per hour
         * @param FreqParam Studied Frequency
         * @param Temperature Temperature (Celsius)
         * @param roadSurface roadSurface empty default, NL01 FR01 ..
         * @param Ts_stud A limited period Ts (in months) over the year where a average proportion pm of light vehicles are equipped with studded tyres and during .
         * @param Pm_stud Average proportion of vehicles equipped with studded tyres
         * @param Junc_dist Distance to junction
         * @param Junc_type Type of junction ((k = 1 for a crossing with traffic lights ; k = 2 for a roundabout)
         */
        // Compute day average level
        double[] ld = new double[PropagationProcessPathData.freq_lvl.size()];
        double lvPerHour = 0;
        double mvPerHour = 0;
        double hgvPerHour = 0;
        double wavPerHour = 0;
        double wbvPerHour = 0;
        double Temperature = 20.0d;
        String roadSurface = "FR_R2";
        double Ts_stud = 0.5;
        double Pm_stud = 4;
        double Junc_dist = 0;
        int Junc_type = 0;
        double slopePercentage = 0;
        double speedLv = speed_lv;
        double speedMv = speed_lv;
        double speedHgv = speed_lv;
        double speedWav = speed_lv;
        double speedWbv = speed_lv;
        for (int h = LDAY_START_HOUR; h < LDAY_STOP_HOUR; h++) {
            lvPerHour = tmja * (1- HV_PERCENTAGE) * (lv_hourly_distribution[h] / 100.0);
            hgvPerHour = tmja * HV_PERCENTAGE * (hv_hourly_distribution[h] / 100.0);
            int idFreq = 0;
            for(int freq : PropagationProcessPathData.freq_lvl) {
                RSParametersCnossos rsParametersCnossos = new RSParametersCnossos(speedLv, speedMv, speedHgv, speedWav,
                        speedWbv, lvPerHour, mvPerHour, hgvPerHour, wavPerHour, wbvPerHour, freq, Temperature,
                        roadSurface, Ts_stud, Pm_stud, Junc_dist, Junc_type);
                rsParametersCnossos.setSpeedFromRoadCaracteristics(speed_lv, speed_lv, false, speed_lv, roadType);
                ld[idFreq++] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(rsParametersCnossos));
            }
        }
        // Average
        for(int i=0; i<ld.length; i++) {
            ld[i] = ld[i] / (LDAY_STOP_HOUR - LDAY_START_HOUR);
        }
        wjSourcesD.add(ld);

    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId);
    }
}

class TrafficPropagationProcessDataFactory implements PointNoiseMap.PropagationProcessDataFactory {
    @Override
    public PropagationProcessData create(FastObstructionTest freeFieldFinder) {
        return new TrafficPropagationProcessData(freeFieldFinder);
    }
}


def static Connection openPostgreSQLDataStoreConnection() {
    Store store = new GeoServer().catalog.getStore("h2gisdb")
    JDBCDataStore jdbcDataStore = (JDBCDataStore) store.getDataStoreInfo().getDataStore(null)
    return jdbcDataStore.getDataSource().getConnection()
}

def static exportScene(String name, FastObstructionTest manager, ComputeRaysOut result) throws IOException {
    try {
        FileOutputStream outData = new FileOutputStream(name);
        KMLDocument kmlDocument = new KMLDocument(outData);
        kmlDocument.setInputCRS("EPSG:2154");
        kmlDocument.writeHeader();
        if(manager != null) {
            kmlDocument.writeTopographic(manager.getTriangles(), manager.getVertices());
        }
        if(result != null) {
            kmlDocument.writeRays(result.getPropagationPaths());
        }
        if(manager != null && manager.isHasBuildingWithHeight()) {
            kmlDocument.writeBuildings(manager);
        }
        kmlDocument.writeFooter();
    } catch (XMLStreamException | CRSException ex) {
        throw new IOException(ex);
    }
}

def run(input) {

    String sources_table_name = "SOURCES"
    if (input['sourcesTableName']){sources_table_name = input['sourcesTableName']}
    sources_table_name = sources_table_name.toUpperCase()
    
    String resultPath = input['resultPath']

    String receivers_table_name = "RECEIVERS"
    if (input['receiversTableName']){receivers_table_name = input['receiversTableName']}
    receivers_table_name = receivers_table_name.toUpperCase()
    
    String building_table_name = "BUILDINGS"
    if (input['buildingTableName']){building_table_name = input['buildingTableName']}
    building_table_name = building_table_name.toUpperCase()

    String dem_table_name = "DEM"
    if (input['demTableName']){building_table_name = input['demTableName']}
    dem_table_name = dem_table_name.toUpperCase()

    String ground_table_name = "GROUND"
    if (input['groundTableName']){ground_table_name = input['groundTableName']}
    ground_table_name = ground_table_name.toUpperCase()

    int reflexion_order = 0
    if (input['reflexionOrder']){reflexion_order = Integer.valueOf(input['reflexionOrder'])}
    
    double max_src_dist = 200
    if (input['maxSrcDistance']){max_src_dist = Double.valueOf(input['maxSrcDistance'])}
    
    double max_ref_dist = 50
    if (input['maxRefDistance']){max_ref_dist = Double.valueOf(input['maxRefDistance'])}
    
    double wall_alpha = 0.1
    if (input['wallAlpha']){wall_alpha = Double.valueOf(input['wallAlpha'])}
    
    int n_thread = 1
    if (input['threadNumber']){n_thread = Integer.valueOf(input['threadNumber'])}

    boolean compute_vertical_diffraction = false
    if (input['computeVertical']){compute_vertical_diffraction = input['computeVertical']}
    
    boolean compute_horizontal_diffraction = false
    if (input['computeHorizontal']){compute_horizontal_diffraction = input['computeHorizontal']}

    List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>() // ca c'est la table avec les attenuations
    ArrayList<PropagationPath> propaMap2 = new ArrayList<>() // ca c'est la table avec tous les rayons, attention gros espace memoire !

    // ----------------------------------
    // Et la on commence la boucle sur les simus
    // ----------------------------------

    System.out.println("Run ...")
    
    // Open connection to database
    Connection connection = openPostgreSQLDataStoreConnection()

    //Need to change the ConnectionWrapper to WpsConnectionWrapper to work under postgis database
    connection = new ConnectionWrapper(connection)

    System.out.println("Connection to the database ok ...")
    // Init NoiseModelling      
    PointNoiseMap pointNoiseMap = new PointNoiseMap(building_table_name, sources_table_name, receivers_table_name)
    pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
    pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
    pointNoiseMap.setSoundReflectionOrder(reflexion_order)
    // Building height field name
    pointNoiseMap.setHeightField("HEIGHT")
    // Import table with Snow, Forest, Grass, Pasture field polygons. Attribute G is associated with each polygon
    //pointNoiseMap.setSoilTableName(ground_table_name);
    // Point cloud height above sea level POINT(X Y Z)
    //pointNoiseMap.setDemTable(dem_table_name);
    // Do not propagate for low emission or far away sources.
    // error in dB
    pointNoiseMap.setMaximumError(0.1d);

    pointNoiseMap.setMaximumPropagationDistance(max_src_dist)
    pointNoiseMap.setMaximumReflectionDistance(max_ref_dist)
    pointNoiseMap.setWallAbsorption(wall_alpha)
    pointNoiseMap.setThreadCount(n_thread)


    // Init custom input in order to compute more than just attenuation

    TrafficPropagationProcessDataFactory trafficPropagationProcessDataFactory = new TrafficPropagationProcessDataFactory();
    pointNoiseMap.setPropagationProcessDataFactory(trafficPropagationProcessDataFactory);
    RootProgressVisitor progressLogger = new RootProgressVisitor(1, true, 1);

    System.out.println("Init Map ...")
    pointNoiseMap.initialize(connection, new EmptyProgressVisitor());

    // Set of already processed receivers
    Set<Long> receivers = new HashSet<>();
    ProgressVisitor progressVisitor = progressLogger.subProcess(pointNoiseMap.getGridDim()*pointNoiseMap.getGridDim());
    
    long start = System.currentTimeMillis();
    System.out.println("Rec\tSource\tLevel");
    System.out.println("Start ...")
    // Iterate over computation areas
    for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
        for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
            // Run ray propagation
            IComputeRaysOut out = pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers);
            // Return results with level spectrum for each source/receiver tuple
            if(out instanceof ComputeRaysOut) {
                ComputeRaysOut cellStorage = (ComputeRaysOut) out;
                exportScene(String.format(resultPath+"/scene_%d_%d.kml", i, j), cellStorage.inputData.freeFieldFinder, cellStorage);
                cellStorage.receiversAttenuationLevels.each { v ->
                    double globalDbValue = ComputeRays.wToDba(ComputeRays.sumArray(ComputeRays.dbaToW(v.value)));
                    System.out.println(String.format("%d\t%d\t%.2f", v.receiverId, v.sourceId, globalDbValue));
                }
            }
        }
    }

    

    long computationTime = System.currentTimeMillis() - start;
   
    return [result : "Calculation Done !"]

    
}