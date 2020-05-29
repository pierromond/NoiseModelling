package org.noise_planet.noisemodelling.propagation;

import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class EvaluateAttenuationThirdOctaveTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EvaluateAttenuationThirdOctaveTest.class);

    private static final double ERROR_EPSILON_very_low = 0.1;


    private static double[] addArray(double[] first, double[] second) {
        int length = first.length < second.length ? first.length
                : second.length;
        double[] result = new double[length];

        for (int i = 0; i < length; i++) {
            result[i] = first[i] + second[i];
        }

        return result;
    }
    /**
     * Test TC01 -- Reflecting ground (G = 0)
     */
    @Test
    public void TC01_thirdOctave()  throws LayerDelaunayError , IOException {
        GeometryFactory factory = new GeometryFactory();
        //Scene dimension
        Envelope cellEnvelope = new Envelope(new Coordinate(-300., -300., 0.), new Coordinate(300, 300, 0.));

        //Create obstruction test object
        MeshBuilder mesh = new MeshBuilder();

        mesh.finishPolygonFeeding(cellEnvelope);

        //Retrieve Delaunay triangulation of scene
        FastObstructionTest manager = new FastObstructionTest(mesh.getPolygonWithHeight(), mesh.getTriangles(),
                mesh.getTriNeighbors(), mesh.getVertices());

        PropagationProcessData rayData = new PropagationProcessData(manager);
        rayData.addReceiver(new Coordinate(200, 50, 4));
        rayData.addSource(factory.createPoint(new Coordinate(10, 10, 1)));
        rayData.setComputeHorizontalDiffraction(true);
        rayData.addSoilType(new GeoWithSoilType(factory.toGeometry(new Envelope(0, 250, -20, 80)), 0));
        rayData.setComputeVerticalDiffraction(true);

        PropagationProcessPathData attData = new PropagationProcessPathData();
        List<Integer> freq_lvl = Arrays.asList(50,63,80,100, 125,160, 200,250,315,400 ,500,630,800, 1000,1250,1600, 2000,2500,3150, 4000,5000,6300, 8000,10000);
        List<Double> freq_lvl_exact = Arrays.asList(50.0, 63.0957,80.0,100.0, 125.8925,160.0,200.0 ,251.1888,315.0,400.0, 501.1872,630.0,800.0, 1000.0,1250.0,1600.0, 1995.26231,2500.0,3150.0, 3981.07171,5000.0,6300.0 ,7943.28235,10000.0);
        attData.setHumidity(70);
        attData.setTemperature(10);
        attData.setFreq_lvl(freq_lvl);
        attData.setFreq_lvl_exact(freq_lvl_exact);

        ComputeRaysOut propDataOut = new ComputeRaysOut(true, attData);
        ComputeRays computeRays = new ComputeRays(rayData);
        computeRays.setThreadCount(1);
        computeRays.run(propDataOut);

        double[] L = addArray(propDataOut.getVerticesSoundLevel().get(0).value, new double[]{93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93,93});
        assertArrayEquals(  new double[]{39.96,39.95,39.94,39.92,39.89,39.86,39.82,39.77,39.72,39.66,39.60,39.52,39.41,39.26,39.04,38.65,38.10,37.23,35.83,33.61,30.26,25.08,17.28,5.97},L, ERROR_EPSILON_very_low);
    }


}