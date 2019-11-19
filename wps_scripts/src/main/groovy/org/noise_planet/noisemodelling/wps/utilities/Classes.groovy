/**
 * @Author Aumond Pierre
 */

package org.noise_planet.noisemodelling.wps.utilities

import groovy.sql.Sql
import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceDynamic
import org.noise_planet.noisemodelling.emission.RSParametersDynamic

import java.sql.SQLException

import org.locationtech.jts.geom.Coordinate

import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream


import org.noise_planet.noisemodelling.emission.EvaluateRoadSourceCnossos
import org.noise_planet.noisemodelling.emission.RSParametersCnossos
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath

import java.util.concurrent.ConcurrentLinkedDeque
import org.h2gis.utilities.SpatialResultSet
import org.locationtech.jts.geom.Geometry
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.cts.crs.CRSException
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.KMLDocument

import javax.xml.stream.XMLStreamException
import org.noise_planet.noisemodelling.propagation.FastObstructionTest
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap


title = 'Classes'
description = 'Classes'

inputs = []

outputs = [tableNameCreated: [name: 'tableNameCreated', title: 'tableNameCreated', type: String.class]]

def run(input) {

    return [tableNameCreated: "Process done. Table of receivers created !"]
}

/**
 *
 */

class DynamicProcessData {

    static double[] readDroneFile(String filename, int theta, int phi)
            throws Exception {
        String line = null
        //System.out.println("line "+ phi+ ":" + theta)
        double[] lvl = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

        // wrap a BufferedReader around FileReader
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filename))

        // use the readLine method of the BufferedReader to read one line at a time.
        // the readLine method returns null when there is nothing else to read.
        int k = 1
        int ntheta = 0
        int nphi = 0

        while ((line = bufferedReader.readLine()) != null) {
            if (k == 4) {
                String[] values = line.split("  ")
                nphi = Integer.valueOf(values[1])
                if (phi == nphi) phi = 0
            }
            if (k == 6) {
                String[] values = line.split("  ")
                ntheta = Integer.valueOf(values[1])
                if (theta == ntheta) theta = 0
            }
            if (k == (14 + (nphi + 2) * theta + phi)) {
                //System.out.println("line "+ (14+(nphi+2)*theta+phi))
                String[] values = line.split("     ")

                double[] parsed = new double[values.length]
                for (int i = 0; i < values.length; i++) parsed[i] = Double.valueOf(values[i])
                lvl = Utilities.thirdOctaveToOctave(parsed)

                break
            }
            k++
        }

        // close the BufferedReader when we're done
        bufferedReader.close()
        return lvl
    }

    double[] getDroneLevel(String tablename, Sql sql, int t, int idSource, double theta, double phi) throws SQLException {
        double[] res_d = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

        // memes valeurs d e et n
        sql.eachRow('SELECT PK, speed,flightmode,t FROM ' + tablename + ' WHERE PK = ' + idSource.toString() + ' AND T = ' + t.toString() + ';') { row ->

            int speed = (int) row[1]
            int thetathird = (int) Math.round(theta / 3)
            int phithird = (int) Math.round(phi / 3)
            String regimen = row[2]
            int id = (int) row[0]
            //System.out.println("Source :" + id)
            int time = (int) row[3]
            res_d = readDroneFile("D:\\aumond\\Documents\\PROJETS\\2019_2020_DRONE\\Raw_Data\\ASCII_Noise_" + speed + "kts_0deg_Step3deg.txt", thetathird, phithird)
        }
        //System.out.println(res_d)

        return res_d
    }

    double[] getDBLevel(String tablename, Sql sql, int t, int idSource) throws SQLException {
        double[] res_d = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        // memes valeurs d e et n
        sql.eachRow('SELECT id, the_geom,\n' +
                'db_m63,db_m125,db_m250,db_m500,db_m1000,db_m2000,db_m4000,db_m8000,t FROM ' + tablename + ' WHERE ID = ' + idSource.toString() + ' AND T = ' + t.toString() + ';') { row ->
            int id = (int) row[0]
            //System.out.println("Source :" + id)
            Geometry the_geom = row[1]
            def db_m63 = row[2]
            def db_m125 = row[3]
            def db_m250 = row[4]
            def db_m500 = row[5]
            def db_m1000 = row[6]
            def db_m2000 = row[7]
            def db_m4000 = row[8]
            def db_m8000 = row[9]
            int time = (int) row[10]


            res_d = [db_m63, db_m125, db_m250, db_m500, db_m1000, db_m2000, db_m4000, db_m8000]

        }

        return res_d
    }


    double[] getCarsLevel(int t, int idSource) throws SQLException {
        double[] res_d = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        double[] res_TV = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        double[] res_PL = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        def list = [63, 125, 250, 500, 1000, 2000, 4000, 8000]
        // memes valeurs d e et n


        def random = Math.random()
        if (random < TV.get(idSource)) {
            int kk = 0
            for (f in list) {

                double speed = SPEED.get(idSource)
                int acc = 0
                int FreqParam = f
                double Temperature = 20
                int RoadSurface = 0
                boolean Stud = true
                double Junc_dist = 200
                int Junc_type = 1
                int veh_type = 1
                int acc_type = 1
                double LwStd = 1
                int VehId = 10

                RSParametersDynamic rsParameters = new RSParametersDynamic(speed, acc, veh_type, acc_type, FreqParam, Temperature, RoadSurface, Stud, Junc_dist, Junc_type, LwStd, VehId)
                rsParameters.setSlopePercentage(0)

                res_TV[kk] = EvaluateRoadSourceDynamic.evaluate(rsParameters)
                kk++
            }

        }
        if (random < PL.get(idSource)) {
            int kk = 0
            for (f in list) {
                double speed = SPEED.get(idSource)
                int acc = 0
                int FreqParam = f
                double Temperature = 20
                int RoadSurface = 0
                boolean Stud = true
                double Junc_dist = 200
                int Junc_type = 1
                int veh_type = 3
                int acc_type = 1
                double LwStd = 1
                int VehId = 10

                RSParametersDynamic rsParameters = new RSParametersDynamic(speed, acc, veh_type, acc_type, FreqParam, Temperature, RoadSurface, Stud, Junc_dist, Junc_type, LwStd, VehId)
                rsParameters.setSlopePercentage(0)

                res_PL[kk] = EvaluateRoadSourceDynamic.evaluate(rsParameters)
                kk++
            }
        }
        int kk = 0
        for (f in list) {
            res_d[kk] = 10 * Math.log10(
                    (1.0 / 2.0) *
                            (Math.pow(10, (10 * Math.log10(Math.pow(10, res_TV[kk] / 10))) / 10)
                                    + Math.pow(10, (10 * Math.log10(Math.pow(10, res_PL[kk] / 10))) / 10)
                            )
            )
            kk++
        }




        return res_d
    }

    void setProbaTable(String tablename, Sql sql) {
        //////////////////////
        // Import file text
        //////////////////////
        int i_read = 0;
        // Remplissage des variables avec le contenu du fichier plan d'exp
        sql.eachRow('SELECT PK, SPEED, DENSITY_TV, DENSITY_PL FROM ' + tablename + ';') { row ->
            int pk = row[0].toInteger()
            SPEED.put(pk, row[1].toFloat())
            TV.put(pk, row[2].toFloat())
            PL.put(pk, row[3].toFloat())

        }

    }
}

/**
 * Read source database and compute the sound emission spectrum of roads sources*/
class TrafficPropagationProcessDataDEN extends PropagationProcessData {
    // Lden values
    public List<double[]> wjSourcesDEN = new ArrayList<>();
    public Map<Long, Integer> SourcesPk = new HashMap<>();


    public TrafficPropagationProcessDataDEN(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder);
    }

    int idSource = 0

    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {
        super.addSource(pk, geom, rs)
        SourcesPk.put(pk, idSource++)

        // Read average 24h traffic
        double[] ld = [ComputeRays.dbaToW(rs.getDouble('Ld63')),
                       ComputeRays.dbaToW(rs.getDouble('Ld125')),
                       ComputeRays.dbaToW(rs.getDouble('Ld250')),
                       ComputeRays.dbaToW(rs.getDouble('Ld500')),
                       ComputeRays.dbaToW(rs.getDouble('Ld1000')),
                       ComputeRays.dbaToW(rs.getDouble('Ld2000')),
                       ComputeRays.dbaToW(rs.getDouble('Ld4000')),
                       ComputeRays.dbaToW(rs.getDouble('Ld8000'))]
        double[] le = [ComputeRays.dbaToW(rs.getDouble('Le63')),
                       ComputeRays.dbaToW(rs.getDouble('Le125')),
                       ComputeRays.dbaToW(rs.getDouble('Le250')),
                       ComputeRays.dbaToW(rs.getDouble('Le500')),
                       ComputeRays.dbaToW(rs.getDouble('Le1000')),
                       ComputeRays.dbaToW(rs.getDouble('Le2000')),
                       ComputeRays.dbaToW(rs.getDouble('Le4000')),
                       ComputeRays.dbaToW(rs.getDouble('Le8000'))]
        double[] ln = [ComputeRays.dbaToW(rs.getDouble('Ln63')),
                       ComputeRays.dbaToW(rs.getDouble('Ln125')),
                       ComputeRays.dbaToW(rs.getDouble('Ln250')),
                       ComputeRays.dbaToW(rs.getDouble('Ln500')),
                       ComputeRays.dbaToW(rs.getDouble('Ln1000')),
                       ComputeRays.dbaToW(rs.getDouble('Ln2000')),
                       ComputeRays.dbaToW(rs.getDouble('Ln4000')),
                       ComputeRays.dbaToW(rs.getDouble('Ln8000'))]

        double[] lden = new double[PropagationProcessPathData.freq_lvl.size()]
        int idFreq = 0
        for (int freq : PropagationProcessPathData.freq_lvl) {
            lden[idFreq++] = (12 * ld[idFreq] +
                    4 * ComputeRays.dbaToW(ComputeRays.wToDba(le[idFreq]) + 5) +
                    8 * ComputeRays.dbaToW(ComputeRays.wToDba(ln[idFreq]) + 10)) / 24.0
        }

        wjSourcesDEN.add(lden)


    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesDEN.get(sourceId);
    }
}


class TrafficPropagationProcessDataDENFactory implements PointNoiseMap.PropagationProcessDataFactory {
    @Override
    public PropagationProcessData create(FastObstructionTest freeFieldFinder) {
        return new TrafficPropagationProcessDataDEN(freeFieldFinder);
    }
}

/** Read source database and compute the sound emission spectrum of roads sources **/

class DynamicPropagationProcessData extends PropagationProcessData {

    protected List<double[]> wjSourcesD = new ArrayList<>()

    public DynamicPropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder)
    }

    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {

        super.addSource(pk, geom, rs)

        Geometry the_geom = rs.getGeometry("the_geom")
        double db_m63 = rs.getDouble("db_m63")
        double db_m125 = rs.getDouble("db_m125")
        double db_m250 = rs.getDouble("db_m250")
        double db_m500 = rs.getDouble("db_m500")
        double db_m1000 = rs.getDouble("db_m1000")
        double db_m2000 = rs.getDouble("db_m2000")
        double db_m4000 = rs.getDouble("db_m4000")
        double db_m8000 = rs.getDouble("db_m8000")
        int t = rs.getInt("T")
        int id = rs.getInt("ID")

        double[] res_d = new double[PropagationProcessPathData.freq_lvl.size()]

        res_d = [db_m63, db_m125, db_m250, db_m500, db_m1000, db_m2000, db_m4000, db_m8000]

        wjSourcesD.add(ComputeRays.dbaToW(res_d))
    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId)
    }


}

/**
 * Read source database and compute the sound emission spectrum of roads sources
 */
class ProbaPropagationProcessData extends PropagationProcessData {

    protected List<double[]> wjSourcesD = new ArrayList<>()

    public ProbaPropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder)
    }

    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {

        super.addSource(pk, geom, rs)

        Geometry the_geom = rs.getGeometry("the_geom")

        double db_m63 = 0
        double db_m125 = 0
        double db_m250 = 0
        double db_m500 = 0
        double db_m1000 = 0
        double db_m2000 = 0
        double db_m4000 = 0
        double db_m8000 = 0
        int id = rs.getInt("PK")

        double[] res_d = [db_m63, db_m125, db_m250, db_m500, db_m1000, db_m2000, db_m4000, db_m8000]
        wjSourcesD.add(ComputeRays.dbaToW(res_d))
    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId)
    }


}


class ProbaPropagationProcessDataFactory implements PointNoiseMap.PropagationProcessDataFactory {

    @Override
    PropagationProcessData create(FastObstructionTest freeFieldFinder) {
        return new ProbaPropagationProcessData(freeFieldFinder)
    }
}



class PropagationPathStorageFactory implements PointNoiseMap.IComputeRaysOutFactory {
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue = new ConcurrentLinkedDeque<>()
    GZIPOutputStream gzipOutputStream
    AtomicBoolean waitForMorePaths = new AtomicBoolean(true)
    public static final int GZIP_CACHE_SIZE = (int) Math.pow(2, 19)
    String workingDir

    void openPathOutputFile(String path) {
        gzipOutputStream = new GZIPOutputStream(new FileOutputStream(path), GZIP_CACHE_SIZE)
        new Thread(new WriteThread(pathQueue, waitForMorePaths, gzipOutputStream)).start()
    }

    void setWorkingDir(String workingDir) {
        this.workingDir = workingDir
    }

    void exportDomain(PropagationProcessData inputData, String path) {
        /*GeoJSONDocument geoJSONDocument = new GeoJSONDocument(new FileOutputStream(path))
        geoJSONDocument.writeHeader()
        geoJSONDocument.writeTopographic(inputData.freeFieldFinder.getTriangles(), inputData.freeFieldFinder.getVertices())
        geoJSONDocument.writeFooter()*/
        KMLDocument kmlDocument
        ZipOutputStream compressedDoc
        System.println("Cellid" + inputData.cellId.toString())
        compressedDoc = new ZipOutputStream(new FileOutputStream(
                String.format("domain_%d.kmz", inputData.cellId)))
        compressedDoc.putNextEntry(new ZipEntry("doc.kml"))
        kmlDocument = new KMLDocument(compressedDoc)
        kmlDocument.writeHeader()
        kmlDocument.setInputCRS("EPSG:2154")
        kmlDocument.setOffset(new Coordinate(0, 0, 0))
        kmlDocument.writeTopographic(inputData.freeFieldFinder.getTriangles(), inputData.freeFieldFinder.getVertices())
        kmlDocument.writeBuildings(inputData.freeFieldFinder)
        kmlDocument.writeFooter()
        compressedDoc.closeEntry()
        compressedDoc.close()
    }

    @Override
    IComputeRaysOut create(PropagationProcessData propagationProcessData, PropagationProcessPathData propagationProcessPathData) {
        exportDomain(propagationProcessData, new File(this.workingDir, String.format("_%d.geojson", propagationProcessData.cellId)).absolutePath)
        return new PropagationPathStorage(propagationProcessData, propagationProcessPathData, pathQueue)
    }

    void closeWriteThread() {
        waitForMorePaths.set(false)
    }

    /**
     * Write paths on disk using a single thread
     */
    static class WriteThread implements Runnable {
        ConcurrentLinkedDeque<PointToPointPaths> pathQueue
        AtomicBoolean waitForMorePaths
        GZIPOutputStream gzipOutputStream

        WriteThread(ConcurrentLinkedDeque<PointToPointPaths> pathQueue, AtomicBoolean waitForMorePaths, GZIPOutputStream gzipOutputStream) {
            this.pathQueue = pathQueue
            this.waitForMorePaths = waitForMorePaths
            this.gzipOutputStream = gzipOutputStream
        }

        @Override
        void run() {
            long exportReceiverRay = 2 // primary key of receiver to export
            KMLDocument kmlDocument

            ZipOutputStream compressedDoc

            compressedDoc = new ZipOutputStream(new FileOutputStream(
                    String.format("domain.kmz")))
            compressedDoc.putNextEntry(new ZipEntry("doc.kml"))
            kmlDocument = new KMLDocument(compressedDoc)
            kmlDocument.writeHeader()
            kmlDocument.setInputCRS("EPSG:2154")
            kmlDocument.setOffset(new Coordinate(0, 0, 0))

            /*PropagationProcessPathData genericMeteoData = new PropagationProcessPathData()
            genericMeteoData.setHumidity(70)
            genericMeteoData.setTemperature(10)
            ComputeRaysOut out = new ComputeRaysOut(false, genericMeteoData)
*/
            DataOutputStream dataOutputStream = new DataOutputStream(gzipOutputStream)
            while (waitForMorePaths.get()) {
                while (!pathQueue.isEmpty()) {
                    PointToPointPaths paths = pathQueue.pop()
                    paths.writePropagationPathListStream(dataOutputStream)

                    if (paths.receiverId == exportReceiverRay) {
                        // Export rays
                        kmlDocument.writeRays(paths.getPropagationPathList())

                    }

                }
                Thread.sleep(10)
            }
            dataOutputStream.flush()
            gzipOutputStream.close()
            kmlDocument.writeFooter()
            compressedDoc.closeEntry()
            compressedDoc.close()


        }
    }
}



class PointToPointPaths {
    ArrayList<PropagationPath> propagationPathList;
    double li
    long sourceId
    long receiverId

    /**
     * Writes the content of this object into <code>out</code>.
     * @param out the stream to write into
     * @throws java.io.IOException if an I/O-error occurs
     */
    void writePropagationPathListStream(DataOutputStream out) throws IOException {

        out.writeLong(receiverId)
        out.writeLong(sourceId)
        out.writeDouble(li)
        out.writeInt(propagationPathList.size())
        for (PropagationPath propagationPath : propagationPathList) {
            propagationPath.writeStream(out);
        }
    }

    /**
     * Reads the content of this object from <code>out</code>. All
     * properties should be set to their default value or to the value read
     * from the stream.
     * @param in the stream to read
     * @throws IOException if an I/O-error occurs
     */
    void readPropagationPathListStream(DataInputStream inputStream) throws IOException {
        if (propagationPathList == null) {
            propagationPathList = new ArrayList<>()
        }

        receiverId = inputStream.readLong()
        sourceId = inputStream.readLong()
        li = inputStream.readDouble()
        int propagationPathsListSize = inputStream.readInt()
        propagationPathList.ensureCapacity(propagationPathsListSize)
        for (int i = 0; i < propagationPathsListSize; i++) {
            PropagationPath propagationPath = new PropagationPath()
            propagationPath.readStream(inputStream)
            propagationPathList.add(propagationPath)
        }
    }

}


class DynamicPropagationProcessDataFactory implements PointNoiseMap.PropagationProcessDataFactory {
    @Override
    PropagationProcessData create(FastObstructionTest freeFieldFinder) {
        return new DynamicPropagationProcessData(freeFieldFinder)
    }
}



/**
 * Collect path computed by ComputeRays and store it into provided queue (with consecutive receiverId)
 * remove receiverpath or put to keep rays or not
 */
class PropagationPathStorage extends ComputeRaysOut {
    // Thread safe queue object
    protected TrafficPropagationProcessData inputData
    ConcurrentLinkedDeque<PointToPointPaths> pathQueue

    PropagationPathStorage(PropagationProcessData inputData, PropagationProcessPathData pathData, ConcurrentLinkedDeque<PointToPointPaths> pathQueue) {
        super(true, pathData, inputData)
        this.inputData = (TrafficPropagationProcessData) inputData
        this.pathQueue = pathQueue
    }

    @Override
    double[] addPropagationPaths(long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
        return new double[0]
    }

    @Override
    double[] computeAttenuation(PropagationProcessPathData pathData, long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
        /*if (receiverId==11 && sourceId == 42171){
            receiverId == 11
        }*/
        double[] attenuation = super.computeAttenuation(pathData, sourceId, sourceLi, receiverId, propagationPath)

        return attenuation
    }

    @Override
    void finalizeReceiver(long l) {

    }

    @Override
    IComputeRaysOut subProcess(int i, int i1) {
        return new PropagationPathStorageThread(this)
    }

    static class PropagationPathStorageThread implements IComputeRaysOut {
        // In order to keep consecutive receivers into the deque an intermediate list is built for each thread
        private List<PointToPointPaths> receiverPaths = new ArrayList<>()
        private PropagationPathStorage propagationPathStorage

        PropagationPathStorageThread(PropagationPathStorage propagationPathStorage) {
            this.propagationPathStorage = propagationPathStorage
        }

        @Override
        double[] addPropagationPaths(long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
            PointToPointPaths paths = new PointToPointPaths()
            paths.li = sourceLi
            paths.receiverId = (propagationPathStorage.inputData.receiversPk.get((int) receiverId).intValue())
            paths.sourceId = propagationPathStorage.inputData.sourcesPk.get((int) sourceId).intValue()
            paths.propagationPathList = new ArrayList<>(propagationPath.size())
            for (PropagationPath path : propagationPath) {
                // Copy path content in order to keep original ids for other method calls
                PropagationPath pathPk = new PropagationPath(path.isFavorable(), path.getPointList(),
                        path.getSegmentList(), path.getSRList())
                pathPk.setIdReceiver((int) paths.receiverId)
                pathPk.setIdSource((int) paths.sourceId)
                paths.propagationPathList.add(pathPk)
                receiverPaths.add(paths)
            }
            double[] aGlobalMeteo = propagationPathStorage.computeAttenuation(propagationPathStorage.genericMeteoData, sourceId, sourceLi, receiverId, propagationPath);
            if (aGlobalMeteo != null && aGlobalMeteo.length > 0) {

                propagationPathStorage.receiversAttenuationLevels.add(new ComputeRaysOut.verticeSL(paths.receiverId, paths.sourceId, aGlobalMeteo))
                return aGlobalMeteo
            } else {
                return new double[0]
            }
        }


        @Override
        void finalizeReceiver(long receiverId) {
            propagationPathStorage.pathQueue.addAll(receiverPaths)
            receiverPaths.clear()
        }

        @Override
        IComputeRaysOut subProcess(int receiverStart, int receiverEnd) {
            return null
        }


    }



    class TrafficPropagationProcessData extends PropagationProcessData {
        // Lden values
        public List<double[]> wjSourcesD = new ArrayList<>()
        public Map<Long, Integer> SourcesPk = new HashMap<>()

        private String AAFD_FIELD_NAME = "AADF";
        // Annual Average Daily Flow (AADF) estimates
        private String ROAD_CATEGORY_FIELD_NAME = "CLAS_ADM";
        double[] lv_hourly_distribution = [0.56, 0.3, 0.21, 0.26, 0.69, 1.8, 4.29, 7.56, 7.09, 5.5, 4.96, 5.04,
                                           5.8, 6.08, 6.23, 6.67, 7.84, 8.01, 7.12, 5.44, 3.45, 2.26, 1.72, 1.12];
        double[] hv_hourly_distribution = [1.01, 0.97, 1.06, 1.39, 2.05, 3.18, 4.77, 6.33, 6.72, 7.32, 7.37, 7.4,
                                           6.16, 6.22, 6.84, 6.74, 6.23, 4.88, 3.79, 3.05, 2.36, 1.76, 1.34, 1.07];
        private static final int LDAY_START_HOUR = 6;
        private static final int LDAY_STOP_HOUR = 18;
        private static final double HV_PERCENTAGE = 0.1;

        TrafficPropagationProcessData(FastObstructionTest freeFieldFinder) {
            super(freeFieldFinder)
        }

        int idSource = 0

        @Override
        void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {
            super.addSource(pk, geom, rs)
            SourcesPk.put(pk, idSource++)

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
            if (road_cat == 1) {
                roadType = 10;
            } else {
                if (road_cat == 2) {
                    roadType = 42;
                } else {
                    roadType = 62;
                }
            }
            double speed_lv = 50;
            if (road_cat == 1) {
                speed_lv = 120;
            } else {
                if (road_cat == 2) {
                    speed_lv = 80;
                }
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
                lvPerHour = tmja * (1 - HV_PERCENTAGE) * (lv_hourly_distribution[h] / 100.0);
                hgvPerHour = tmja * HV_PERCENTAGE * (hv_hourly_distribution[h] / 100.0);
                int idFreq = 0;
                for (int freq : PropagationProcessPathData.freq_lvl) {
                    RSParametersCnossos rsParametersCnossos = new RSParametersCnossos(speedLv, speedMv, speedHgv, speedWav,
                            speedWbv, lvPerHour, mvPerHour, hgvPerHour, wavPerHour, wbvPerHour, freq, Temperature,
                            roadSurface, Ts_stud, Pm_stud, Junc_dist, Junc_type);
                    rsParametersCnossos.setSpeedFromRoadCaracteristics(speed_lv, speed_lv, false, speed_lv, roadType);
                    ld[idFreq++] += ComputeRays.dbaToW(EvaluateRoadSourceCnossos.evaluate(rsParametersCnossos));
                }
            }
            // Average
            for (int i = 0; i < ld.length; i++) {
                ld[i] = ld[i] / (LDAY_STOP_HOUR - LDAY_START_HOUR);
            }
            wjSourcesD.add(ld);

        }

        @Override
        double[] getMaximalSourcePower(int sourceId) {
            return wjSourcesD.get(sourceId)
        }
    }



}

/** Read source database and compute the sound emission spectrum of roads sources **/

class DronePropagationProcessData extends PropagationProcessData {

    protected List<double[]> wjSourcesD = new ArrayList<>()

    public DronePropagationProcessData(FastObstructionTest freeFieldFinder) {
        super(freeFieldFinder)
    }

    @Override
    public void addSource(Long pk, Geometry geom, SpatialResultSet rs) throws SQLException {

        super.addSource(pk, geom, rs)

        Geometry the_geom = rs.getGeometry("the_geom")
        double db_m63 = 90
        double db_m125 = 90
        double db_m250 = 90
        double db_m500 = 90
        double db_m1000 = 90
        double db_m2000 = 90
        double db_m4000 = 90
        double db_m8000 = 90

        double[] res_d = new double[PropagationProcessPathData.freq_lvl.size()]

        res_d = [db_m63, db_m125, db_m250, db_m500, db_m1000, db_m2000, db_m4000, db_m8000]

        wjSourcesD.add(ComputeRays.dbaToW(res_d))
    }

    @Override
    public double[] getMaximalSourcePower(int sourceId) {
        return wjSourcesD.get(sourceId)
    }


}



class DronePropagationProcessDataFactory implements PointNoiseMap.PropagationProcessDataFactory {
    @Override
    PropagationProcessData create(FastObstructionTest freeFieldFinder) {
        return new DronePropagationProcessData(freeFieldFinder)
    }
}



class Utilities {
/**
 * Read source database and compute the sound emission spectrum of roads sources*/
    static double[] thirdOctaveToOctave(double[] values) {
        double[] valueoct = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        for (int i = 0; i < 8; i++) {
            valueoct[i] = 10 * Math.log10(Math.pow(10, values[i * 3 + 1] / 10) + Math.pow(10, values[i * 3 + 2] / 10) + Math.pow(10, values[i * 3 + 3] / 10))
        }
        return valueoct
    }

    static double[] DBToDBA(double[] db) {
        double[] dbA = [-26.2, -16.1, -8.6, -3.2, 0, 1.2, 1.0, -1.1]
        for (int i = 0; i < db.length; ++i) {
            db[i] = db[i] + dbA[i]
        }
        return db

    }

    static double[] sumArraySR(double[] array1, double[] array2) {
        if (array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array");
        } else {
            double[] sum = new double[array1.length];

            for (int i = 0; i < array1.length; ++i) {
                sum[i] = (array1[i]) + (array2[i]);
            }

            return sum;
        }
    }

    static double[] sumLinearArray(double[] array1, double[] array2) {
        if (array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array")
        } else {
            double[] sum = new double[array1.length];

            for (int i = 0; i < array1.length; ++i) {
                sum[i] = array1[i] + array2[i]
            }

            return sum;
        }
    }

    def static exportScene(String name, FastObstructionTest manager, ComputeRaysOut result) throws IOException {
        try {
            FileOutputStream outData = new FileOutputStream(name);
            KMLDocument kmlDocument = new KMLDocument(outData);
            kmlDocument.setInputCRS("EPSG:2154");
            kmlDocument.writeHeader();
            if (manager != null) {
                kmlDocument.writeTopographic(manager.getTriangles(), manager.getVertices());
            }
            if (result != null) {
                kmlDocument.writeRays(result.getPropagationPaths());
            }
            if (manager != null && manager.isHasBuildingWithHeight()) {
                kmlDocument.writeBuildings(manager)
            }
            kmlDocument.writeFooter();
        } catch (XMLStreamException | CRSException ex) {
            throw new IOException(ex)
        }
    }

}

