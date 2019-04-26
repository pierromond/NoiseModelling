/**
 * NoiseMap is a scientific computation plugin for OrbisGIS developed in order to
 * evaluate the noise impact on urban mobility plans. This model is
 * based on the French standard method NMPB2008. It includes traffic-to-noise
 * sources evaluation and sound propagation processing.
 * <p>
 * This version is developed at French IRSTV Institute and at IFSTTAR
 * (http://www.ifsttar.fr/) as part of the Eval-PDU project, funded by the
 * French Agence Nationale de la Recherche (ANR) under contract ANR-08-VILL-0005-01.
 * <p>
 * Noisemap is distributed under GPL 3 license. Its reference contact is Judicaël
 * Picaut <judicael.picaut@ifsttar.fr>. It is maintained by Nicolas Fortin
 * as part of the "Atelier SIG" team of the IRSTV Institute <http://www.irstv.fr/>.
 * <p>
 * Copyright (C) 2011 IFSTTAR
 * Copyright (C) 2011-2012 IRSTV (FR CNRS 2488)
 * <p>
 * Noisemap is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * <p>
 * Noisemap is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with
 * Noisemap. If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.noise_planet.noisemodelling.propagation;

import org.apache.commons.math3.geometry.euclidean.threed.Line;
import org.apache.commons.math3.geometry.euclidean.threed.Plane;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.utilities.jts_utils.CoordinateUtils;
import org.locationtech.jts.algorithm.CGAlgorithms3D;
import org.locationtech.jts.algorithm.ConvexHull;
import org.locationtech.jts.algorithm.LineIntersector;
import org.locationtech.jts.algorithm.RobustLineIntersector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.math.Vector2D;
import org.locationtech.jts.math.Vector3D;
import org.locationtech.jts.triangulate.quadedge.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Nicolas Fortin
 * @author Pierre Aumond
 */
public class ComputeRays {
    // Reject side diffraction if hull length > than direct length
    // because 20 * LOG10(4) = 12 dB, so small contribution in comparison with diffraction on horizontal edge
    // in order to reduce computational cost
    private final static double MAX_RATIO_HULL_DIRECT_PATH = 4;
    private int threadCount;
    private PropagationProcessData data;

    private STRtree rTreeOfGeoSoil;
    private final static Logger LOGGER = LoggerFactory.getLogger(ComputeRays.class);

    public static double[] sumArrayWithPonderation(double[] array1, double[] array2, double p) {
        if(array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array");
        }
        double[] sum = new double[array1.length];
        for (int i = 0; i < array1.length; i++) {
            sum[i] = wToDba(p * dbaToW(array1[i]) + (1 - p) * dbaToW(array2[i]));
        }
        return sum;
    }

    /**
     * energetic Sum of dBA array
     * @param array1
     * @param array2
     * @return
     */
    public static double[] sumDbArray(double[] array1, double[] array2) {
        if(array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array");
        }
        double[] sum = new double[array1.length];
        for (int i = 0; i < array1.length; i++) {
            sum[i] = wToDba(dbaToW(array1[i]) + dbaToW(array2[i]));
        }
        return sum;
    }

    /**
     * Multiply component of two same size array
     * @param array1
     * @param array2
     * @return
     */
    public static double[] multArray(double[] array1, double[] array2) {
        if(array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array");
        }
        double[] sum = new double[array1.length];
        for (int i = 0; i < array1.length; i++) {
            sum[i] = array1[i] * array2[i];
        }
        return sum;
    }

    public static double sumArray(int nbfreq, double energeticSum[]) {
        double globlvl = 0;
        for (int idfreq = 0; idfreq < nbfreq; idfreq++) {
            globlvl += energeticSum[idfreq];
        }
        return globlvl;
    }

    public static double sumArray(double energeticSum[]) {
        double globlvl = 0;
        for (int idfreq = 0; idfreq < energeticSum.length; idfreq++) {
            globlvl += energeticSum[idfreq];
        }
        return globlvl;
    }

    public ComputeRays(PropagationProcessData data) {
        this.data = data;
        Runtime runtime = Runtime.getRuntime();
        this.threadCount = runtime.availableProcessors();
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    /**
     * Update ground Z coordinates of sound sources and receivers absolute to sea levels
     */
    public void makeRelativeZToAbsolute() {
        AbsoluteCoordinateSequenceFilter filter = new AbsoluteCoordinateSequenceFilter(data.freeFieldFinder, true);
        for (Geometry source : data.sourceGeometries) {
            source.apply(filter);
        }
        CoordinateSequence sequence = new CoordinateArraySequence(data.receivers.toArray(new Coordinate[data.receivers.size()]));
        for (int i = 0; i < sequence.size(); i++) {
            filter.filter(sequence, i);
        }
        data.receivers = Arrays.asList(sequence.toCoordinateArray());
    }

    public static double dbaToW(double dBA) {
        return Math.pow(10., dBA / 10.);
    }

    public static double[] dbaToW(double[] dBA) {
        double[] ret = new double[dBA.length];
        for(int i=0; i<dBA.length; i++) {
            ret[i] = dbaToW(dBA[i]);
        }
        return ret;
    }

    public static double wToDba(double w) {
        return 10 * Math.log10(w);
    }

    public static double[] wToDba(double[] w) {
        double[] ret = new double[w.length];
        for(int i=0; i<w.length; i++) {
            ret[i] = wToDba(w[i]);
        }
        return ret;
    }

    /**
     * @param startPt Compute the closest point on lineString with this coordinate,
     *                use it as one of the splitted points
     * @return li coefficient to apply to equivalent source point from the sound power per metre set on linear source
     */
    /**
     *
     * @param geom Geometry
     * @param segmentSizeConstraint Maximal distance between points
     * @param[out] pts computed points
     * @return Fixed distance between points
     */
    public static double splitLineStringIntoPoints(LineString geom, double segmentSizeConstraint,
                                             List<Coordinate> pts) {
        // If the linear sound source length is inferior than half the distance between the nearest point of the sound
        // source and the receiver then it can be modelled as a single point source
        double geomLength = geom.getLength();
        if(geomLength < segmentSizeConstraint) {
           // Return mid point
            Coordinate[] points = geom.getCoordinates();
            double segmentLength = 0;
            final double targetSegmentSize = geomLength / 2.0;
            for (int i = 0; i < points.length - 1; i++) {
                Coordinate a = points[i];
                final Coordinate b = points[i + 1];
                double length = a.distance3D(b);
                if(length + segmentLength > targetSegmentSize) {
                    double segmentLengthFraction = (targetSegmentSize - segmentLength) / length;
                    Coordinate midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                            a.y + segmentLengthFraction * (b.y - a.y),
                            a.z + segmentLengthFraction * (b.z - a.z));
                    pts.add(midPoint);
                    break;
                }
                segmentLength += length;
            }
            return geom.getLength();
        } else {
            double targetSegmentSize = geomLength / Math.ceil(geomLength / segmentSizeConstraint);
            Coordinate[] points = geom.getCoordinates();
            double segmentLength = 0.;

            // Mid point of segmented line source
            Coordinate midPoint = null;
            for (int i = 0; i < points.length - 1; i++) {
                Coordinate a = points[i];
                final Coordinate b = points[i + 1];
                double length = a.distance3D(b);
                while (length + segmentLength > targetSegmentSize) {
                    //LineSegment segment = new LineSegment(a, b);
                    double segmentLengthFraction = (targetSegmentSize - segmentLength) / length;
                    Coordinate splitPoint = new Coordinate();
                    splitPoint.x = a.x + segmentLengthFraction * (b.x - a.x);
                    splitPoint.y = a.y + segmentLengthFraction * (b.y - a.y);
                    splitPoint.z = a.z + segmentLengthFraction * (b.z - a.z);
                    if(midPoint == null && length + segmentLength > targetSegmentSize / 2) {
                        segmentLengthFraction = (targetSegmentSize / 2.0 - segmentLength) / length;
                        midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                                a.y + segmentLengthFraction * (b.y - a.y),
                                a.z + segmentLengthFraction * (b.z - a.z));
                    }
                    pts.add(midPoint);
                    a = splitPoint;
                    length = a.distance3D(b);
                    segmentLength = 0;
                    midPoint = null;
                }
                if(midPoint == null && length + segmentLength > targetSegmentSize / 2) {
                    double segmentLengthFraction = (targetSegmentSize / 2.0 - segmentLength) / length;
                    midPoint = new Coordinate(a.x + segmentLengthFraction * (b.x - a.x),
                            a.y + segmentLengthFraction * (b.y - a.y),
                            a.z + segmentLengthFraction * (b.z - a.z));
                }
                segmentLength += length;
            }
            if(midPoint != null) {
                pts.add(midPoint);
            }
            return targetSegmentSize;
        }
    }


    public List<PropagationPath> computeReflexion(Coordinate receiverCoord,
                                            Coordinate srcCoord, boolean favorable, List<FastObstructionTest.Wall> nearBuildingsWalls,
                                            List<PropagationDebugInfo> debugInfo) {
        // Compute receiver mirror
        LineSegment srcReceiver = new LineSegment(srcCoord, receiverCoord);
        LineIntersector linters = new RobustLineIntersector();

        Coordinate lastPoint = new Coordinate();

        Coordinate reflectionPt;
        List<PropagationPath> propagationPaths_all = new ArrayList<>();


        MirrorReceiverIterator.It mirroredReceivers = new MirrorReceiverIterator.It(receiverCoord, nearBuildingsWalls,
                srcReceiver, data.maxRefDist, data.reflexionOrder, data.maxSrcDist);

        for (MirrorReceiverResult receiverReflection : mirroredReceivers) {

            PropagationPath propagationPath = new PropagationPath();
            List<PropagationPath.PointPath> points = new ArrayList<PropagationPath.PointPath>();
            List<PropagationPath.SegmentPath> segments = new ArrayList<PropagationPath.SegmentPath>();
            List<PropagationPath.SegmentPath> srPath = new ArrayList<PropagationPath.SegmentPath>();
            int refcount = 0;

            boolean validReflection = false;
            MirrorReceiverResult receiverReflectionCursor = receiverReflection;
            // Test whether intersection point is on the wall
            // segment or not
            Coordinate destinationPt = new Coordinate(srcCoord);

            FastObstructionTest.Wall seg = nearBuildingsWalls.get(receiverReflection.getWallId());
            linters.computeIntersection(seg.p0, seg.p1,
                    receiverReflection.getReceiverPos(),
                    destinationPt);
            PropagationDebugInfo propagationDebugInfo = null;
            if (debugInfo != null) {
                propagationDebugInfo = new PropagationDebugInfo(new LinkedList<>(Arrays.asList(srcCoord)), new double[data.freq_lvl.length]);
            }
            // While there is a reflection point on another wall. And intersection point is in the wall z bounds.
            reflectionPt = new Coordinate(linters.getIntersection(0));
            while (linters.hasIntersection() && MirrorReceiverIterator.wallPointTest(seg, destinationPt)) {
                // There are a probable reflection point on the segment
                if (reflectionPt.equals(destinationPt)) {
                    break;
                }
                // Compute Z interpolation
                reflectionPt.setOrdinate(Coordinate.Z, Vertex.interpolateZ(linters.getIntersection(0),
                        receiverReflectionCursor.getReceiverPos(), destinationPt));

                // Test if there is no obstacles between the
                // reflection point and old reflection pt (or source position)
                validReflection = (Double.isNaN(receiverReflectionCursor.getReceiverPos().z) ||
                        Double.isNaN(reflectionPt.z) || Double.isNaN(destinationPt.z) || seg.getBuildingId() == 0
                        || reflectionPt.z < data.freeFieldFinder.getBuildingRoofZ(seg.getBuildingId())
                        || reflectionPt.z > data.freeFieldFinder.getHeightAtPosition(reflectionPt)
                        || destinationPt.z > data.freeFieldFinder.getHeightAtPosition(destinationPt));
                if (validReflection) // Reflection point can see
                // source or its image
                // source or its image
                {

                    if (propagationDebugInfo != null) {
                        propagationDebugInfo.getPropagationPath().add(0, reflectionPt);
                    }
                    if (receiverReflectionCursor
                            .getParentMirror() == null) { // Direct to the receiver
                        validReflection = data.freeFieldFinder
                                .isFreeField(reflectionPt,
                                        receiverCoord);
                        break; // That was the last reflection
                    } else {
                        // There is another reflection
                        destinationPt.setCoordinate(reflectionPt);
                        // Move reflection information cursor to a
                        // reflection closer
                        receiverReflectionCursor = receiverReflectionCursor.getParentMirror();
                        // Update intersection data
                        seg = nearBuildingsWalls
                                .get(receiverReflectionCursor
                                        .getWallId());
                        linters.computeIntersection(seg.p0, seg.p1,
                                receiverReflectionCursor
                                        .getReceiverPos(),
                                destinationPt
                        );
                        validReflection = false;
                    }
                } else {
                    break;
                }
            }
            if (validReflection && !Double.isNaN(reflectionPt.z) ) {
                if (propagationDebugInfo != null) {
                    propagationDebugInfo.getPropagationPath().add(0, receiverCoord);
                }
                lastPoint = reflectionPt;

                // A path has been found
                List<PropagationPath> propagationPaths = directPath(destinationPt, reflectionPt, data.isComputeVerticalDiffraction(),false,  debugInfo);

                if (propagationPaths.size() > 0 ) {
                    refcount +=1;
                    propagationPath = propagationPaths.get(0);
                    propagationPath.getPointList().get(propagationPath.getPointList().size() - 1).setType(PropagationPath.PointPath.POINT_TYPE.REFL);
                    propagationPath.getPointList().get(propagationPath.getPointList().size() - 1).setBuildingId(receiverReflection.getBuildingId());
                    propagationPath.getPointList().get(propagationPath.getPointList().size() - 1).setAlphaWall(data.freeFieldFinder.getBuildingAlpha(receiverReflection.getBuildingId()));

                    if (refcount > 1) {
                        propagationPath.getPointList().remove(0);
                    }
                    points.addAll(propagationPath.getPointList());
                    segments.addAll(propagationPath.getSegmentList());
                }


                if (propagationDebugInfo != null) {
                    debugInfo.add(propagationDebugInfo);
                }


            }
            if (refcount > 0 ) {
                List<PropagationPath> propagationPaths = directPath(lastPoint, receiverCoord, data.isComputeVerticalDiffraction(),false, debugInfo);
                if (propagationPaths.size() > 0 ) {
                    propagationPath = propagationPaths.get(0);
                    propagationPath.getPointList().remove(0);
                    points.addAll(propagationPath.getPointList());
                    segments.addAll(propagationPath.getSegmentList());
                    srPath.add(new PropagationPath.SegmentPath(0.0, new Vector3D(srcCoord, receiverCoord),new Coordinate(0,0,0)));


                    for (int i = 1; i < points.size(); i++) {
                        if (points.get(i).type == PropagationPath.PointPath.POINT_TYPE.DIFH) {
                            if (points.get(i).coordinate.z <= data.freeFieldFinder.getHeightAtPosition(points.get(i).coordinate)) {
                                points.clear();
                                segments.clear();
                                break;
                            }
                        } else if (points.get(i).type == PropagationPath.PointPath.POINT_TYPE.REFL) {
                            if(i < points.size() - 1 ) {
                                points.get(i).coordinate.z = Vertex.interpolateZ(points.get(i).coordinate, points.get(i - 1).coordinate, points.get(i + 1).coordinate);
                                //check if in building && if under floor
                                if (points.get(i).coordinate.z > data.freeFieldFinder.getBuildingRoofZ(points.get(i).getBuildingId())
                                        || points.get(i).coordinate.z <= data.freeFieldFinder.getHeightAtPosition(points.get(i).coordinate)) {
                                    points.clear();
                                    segments.clear();
                                    break;
                                }
                            } else {
                                LOGGER.warn("Invalid state, reflexion point on last point");
                            }
                        }
                    }
                    if (points.size() > 2) {
                        propagationPaths_all.add(new PropagationPath(favorable, points, segments, srPath));
                    }
                }
            }
        }
        return propagationPaths_all;
    }


    private static List<Coordinate> removeDuplicates(List<Coordinate> coordinates) {
        return Arrays.asList(CoordinateUtils.removeDuplicatedCoordinates(
                coordinates.toArray(new Coordinate[coordinates.size()]), false));
    }


    /**
     * @param receiverCoord
     * @param srcCoord
     * @param inters PropagationPath between srcCoord and receiverCoord (or null if must be computed here)
     * @param debugInfo
     */
    public PropagationPath computeFreefield(Coordinate receiverCoord,
                                            Coordinate srcCoord,List<TriIdWithIntersection> inters,
                                            List<PropagationDebugInfo> debugInfo) {

        GeometryFactory factory = new GeometryFactory();
        List<PropagationPath.PointPath> points = new ArrayList<PropagationPath.PointPath>();
        List<PropagationPath.SegmentPath> segments = new ArrayList<PropagationPath.SegmentPath>();
        List<PropagationPath.SegmentPath> srPath = new ArrayList<PropagationPath.SegmentPath>();

        double gPath;
        double totRSDistance = 0.;
        double altR = 0;
        double altS = 0;
        Coordinate projReceiver;
        Coordinate projSource;


        //will give a flag here for soil effect
        final List<GeoWithSoilType> soilTypeList = data.getSoilList();
        LineString RSZone = factory.createLineString(new Coordinate[]{receiverCoord, srcCoord});
        List<EnvelopeWithIndex<Integer>> resultZ0 = rTreeOfGeoSoil.query(RSZone.getEnvelopeInternal());
        if (!resultZ0.isEmpty()) {
            for (EnvelopeWithIndex<Integer> envel : resultZ0) {
                //get the geo intersected
                Geometry geoInter = RSZone.intersection(soilTypeList.get(envel.getId()).getGeo());
                //add the intersected distance with ground effect
                totRSDistance += getIntersectedDistance(geoInter) * soilTypeList.get(envel.getId()).getType();
            }
        }
        // Compute GPath using 2D Length
        gPath = totRSDistance / RSZone.getLength();

        if(inters == null) {
            inters = new ArrayList<>();
            data.freeFieldFinder.computePropagationPath(srcCoord, receiverCoord, false, inters, true);
        }
        List<Coordinate> rSground = data.freeFieldFinder.getGroundProfile(inters);
        altR = rSground.get(inters.size() - 1).z;    // altitude Receiver
        altS = rSground.get(0).z; // altitude Source
        double angle = new LineSegment(rSground.get(0), rSground.get(rSground.size() - 1)).angle();
        rSground = JTSUtility.getNewCoordinateSystem(rSground);

        // Compute mean ground plan
        double[] ab = JTSUtility.getMeanPlaneCoefficients(rSground.toArray(new Coordinate[rSground.size()]));
        Coordinate pInit = new Coordinate();
        Coordinate rotatedReceiver = new Coordinate(rSground.get(rSground.size() - 1));
        rotatedReceiver.setOrdinate(1, receiverCoord.z);
        Coordinate rotatedSource = new Coordinate(rSground.get(0));
        rotatedSource.setOrdinate(1, srcCoord.z);
        projReceiver = JTSUtility.makeProjectedPoint(ab[0], ab[1], rotatedReceiver);
        projSource = JTSUtility.makeProjectedPoint(ab[0], ab[1], rotatedSource);
        pInit = JTSUtility.makeProjectedPoint(ab[0], ab[1], new Coordinate(0,0,0));
        projReceiver = JTSUtility.getOldCoordinateSystem(projReceiver, angle);
        projSource = JTSUtility.getOldCoordinateSystem(projSource, angle);
        pInit = JTSUtility.getOldCoordinateSystem(pInit, angle);

        projReceiver.x = srcCoord.x + projReceiver.x;
        projSource.x = srcCoord.x + projSource.x;
        projReceiver.y = srcCoord.y + projReceiver.y;
        projSource.y = srcCoord.y + projSource.y;
        pInit.x = srcCoord.x + pInit.x;
        pInit.y = srcCoord.y + pInit.y;

        segments.add(new PropagationPath.SegmentPath(gPath, new Vector3D(projSource, projReceiver),pInit));

        points.add(new PropagationPath.PointPath(srcCoord, altS, data.gS, Double.NaN, -1, PropagationPath.PointPath.POINT_TYPE.SRCE));
        points.add(new PropagationPath.PointPath(receiverCoord, altR, data.gS, Double.NaN, -1, PropagationPath.PointPath.POINT_TYPE.RECV));

        if (debugInfo != null) {
            debugInfo.add(new PropagationDebugInfo(Arrays.asList(receiverCoord, srcCoord), new double[data.freq_lvl.length]));
        }

        return new PropagationPath(false, points, segments, segments);

    }


    public PropagationPath computeHorizontalEdgeDiffraction(boolean obstructedSourceReceiver, Coordinate receiverCoord,
                                                            Coordinate srcCoord, List<TriIdWithIntersection> allInterPoints, List<PropagationDebugInfo> debugInfo) {

        List<PropagationPath.PointPath> points = new ArrayList<PropagationPath.PointPath>();
        List<PropagationPath.SegmentPath> segments = new ArrayList<PropagationPath.SegmentPath>();
        List<PropagationPath.SegmentPath> srPath = new ArrayList<PropagationPath.SegmentPath>();
        boolean validDiffraction;

        DiffractionWithSoilEffetZone diffDataWithSoilEffet;


        if (!obstructedSourceReceiver) {
            diffDataWithSoilEffet = data.freeFieldFinder.getPathInverse(receiverCoord, srcCoord);
            validDiffraction = false;
        } else {
            diffDataWithSoilEffet = data.freeFieldFinder.getPath(receiverCoord, srcCoord, allInterPoints);
            // Offset Coordinates by epsilon
            validDiffraction = diffDataWithSoilEffet.getROZone() != null;
        }
        // todo not sure about this part...
        if (validDiffraction) {
                List<Coordinate> offsetPath = new ArrayList<>(diffDataWithSoilEffet.getPath());
                for(int i = 1; i < offsetPath.size() - 1; i++) {
                    Coordinate dest = offsetPath.get(i);
                    Vector2D v = new Vector2D(offsetPath.get(0), dest).normalize().multiply(FastObstructionTest.epsilon);
                    offsetPath.set(i, new Coordinate(dest.x - v.getX(), dest.y - v.getY(), dest.z));
                }
                for (int j = offsetPath.size() - 1; j > 1; j--) {
                    PropagationPath propagationPath1 = computeFreefield(offsetPath.get(j - 1), offsetPath.get(j), null, debugInfo);
                    propagationPath1.getPointList().get(1).setType(PropagationPath.PointPath.POINT_TYPE.DIFH);
                    if (j == offsetPath.size() - 1) {
                        propagationPath1.getPointList().get(0).setCoordinate(offsetPath.get(j));
                        points.add(propagationPath1.getPointList().get(0));
                    }
                    points.add(propagationPath1.getPointList().get(1));
                    segments.addAll(propagationPath1.getSegmentList());
                }

                PropagationPath propagationPath2 = computeFreefield(offsetPath.get(0), offsetPath.get(1),null, debugInfo);
                points.add(propagationPath2.getPointList().get(1));
                segments.add(propagationPath2.getSegmentList().get(0));

        } else {
            PropagationPath propagationPath = computeFreefield(receiverCoord, srcCoord,null, debugInfo);
            points.addAll(propagationPath.getPointList());
            segments.addAll(propagationPath.getSegmentList());
            srPath.addAll(propagationPath.getSRList());
        }

        return new PropagationPath(true, points, segments, srPath);
    }

    public HashSet<Integer> getBuildingsOnPath(Coordinate p1, Coordinate p2) {
        HashSet<Integer> buildingsOnPath = new HashSet<>();
        List<TriIdWithIntersection> propagationPath = new ArrayList<>();
        data.freeFieldFinder.computePropagationPath(p1, p2, false, propagationPath, true);
        if (!propagationPath.isEmpty()) {
            for (TriIdWithIntersection inter : propagationPath) {
                if (inter.getBuildingId() != 0) {
                    buildingsOnPath.add(inter.getBuildingId());
                }
            }
        }
        return buildingsOnPath;
    }


    public static Plane ComputeZeroRadPlane(Coordinate p0, Coordinate p1) {
        org.apache.commons.math3.geometry.euclidean.threed.Vector3D s = new org.apache.commons.math3.geometry.euclidean.threed.Vector3D(p0.x, p0.y, p0.z);
        org.apache.commons.math3.geometry.euclidean.threed.Vector3D r = new org.apache.commons.math3.geometry.euclidean.threed.Vector3D(p1.x, p1.y, p1.z);
        double angle = Math.atan2(p1.y - p0.y, p1.x - p0.x);
        // Compute rPrime, the third point of the plane that is at -PI/2 with SR vector
        org.apache.commons.math3.geometry.euclidean.threed.Vector3D rPrime = s.add(new org.apache.commons.math3.geometry.euclidean.threed.Vector3D(Math.cos(angle - Math.PI / 2),Math.sin(angle - Math.PI / 2),0));
        Plane p = new Plane(r, s, rPrime, 1e-6);
        // Normal of the cut plane should be upward
        if(p.getNormal().getZ() < 0) {
            p.revertSelf();
        }
        return p;
    }


//    public org.apache.commons.math3.geometry.euclidean.threed.Vector3D transform(Plane plane, Coordinate p) {
//        org.apache.commons.math3.geometry.euclidean.twod.Vector2D sp = plane.toSubSpace(p);
//        return new org.apache.commons.math3.geometry.euclidean.threed.Vector3D(sp.getX(), sp.getY(), plane.getOffset(p));
//    }

    public static org.apache.commons.math3.geometry.euclidean.threed.Vector3D CoordinateToVector(Coordinate p) {
        return new org.apache.commons.math3.geometry.euclidean.threed.Vector3D(p.x, p.y, p.z);
    }

    public static List<Coordinate> cutRoofPointsWithPlane(Plane plane, List<Coordinate> roofPts) {
        List<Coordinate> polyCut = new ArrayList<>(roofPts.size());
        Double lastOffset = null;
        for(int idp = 0; idp < roofPts.size(); idp++) {
            double offset = plane.getOffset(CoordinateToVector(roofPts.get(idp)));
            if(lastOffset != null && ((offset >= 0 && lastOffset < 0) || (offset < 0 && lastOffset >= 0))) {
                // Interpolate vector
                org.apache.commons.math3.geometry.euclidean.threed.Vector3D i = plane.intersection(new Line(CoordinateToVector(roofPts.get(idp - 1)),CoordinateToVector(roofPts.get(idp)),FastObstructionTest.epsilon));
                polyCut.add(new Coordinate(i.getX(), i.getY(), i.getZ()));
            }
            if(offset >= 0) {
                org.apache.commons.math3.geometry.euclidean.threed.Vector3D i = plane.intersection(new Line(new org.apache.commons.math3.geometry.euclidean.threed.Vector3D(roofPts.get(idp).x,roofPts.get(idp).y,Double.MIN_VALUE),CoordinateToVector(roofPts.get(idp)),FastObstructionTest.epsilon));
                polyCut.add(new Coordinate(i.getX(), i.getY(), i.getZ()));
            }
            lastOffset = offset;
        }
        return polyCut;
    }
    /**
     *
     * @param left If true return path between p1 and p2; else p2 to p1
     * @param p1 First point
     * @param p2 Second point
     * @return
     */
    public List<Coordinate> computeSideHull(boolean left, Coordinate p1, Coordinate p2) {
        if(p1.equals(p2)) {
            return new ArrayList<>();
        }

        Plane cutPlane = ComputeZeroRadPlane(p1, p2);

        final LineSegment receiverSrc = new LineSegment(p1, p2);
        // Intersection test cache
        Set<LineSegment> freeFieldSegments = new HashSet<>();
        GeometryFactory geometryFactory = new GeometryFactory();

        List<Coordinate> input = new ArrayList<>();

        Coordinate[] coordinates = new Coordinate[0];
        int indexp1 = 0;
        int indexp2 = 0;

        boolean convexHullIntersects = true;
        HashSet<Integer> buildingsOnPath = new HashSet<>();

        input.add(p1);
        input.add(p2);

        for(int i : getBuildingsOnPath(p1, p2)) {
            if(!buildingsOnPath.contains(i)) {
                List<Coordinate> roofPoints = data.freeFieldFinder.getWideAnglePointsByBuilding(i, 0, 2 * Math.PI);
                roofPoints = cutRoofPointsWithPlane(cutPlane, roofPoints);
                if(!roofPoints.isEmpty()) {
                    input.addAll(roofPoints.subList(0, roofPoints.size() - 1));
                    buildingsOnPath.add(i);
                }
            }
        }
        int k;
        while (convexHullIntersects) {
            ConvexHull convexHull = new ConvexHull(input.toArray(new Coordinate[input.size()]), geometryFactory);
            Geometry convexhull = convexHull.getConvexHull();

            if (convexhull.getLength() / p1.distance(p2) > MAX_RATIO_HULL_DIRECT_PATH) {
                return new ArrayList<>();
            }

            convexHullIntersects = false;
            coordinates = convexhull.getCoordinates();

            input.clear();
            input.addAll(Arrays.asList(coordinates));

            indexp1 = -1;
            for (int i = 0; i < coordinates.length - 1; i++) {
                if (coordinates[i].equals(p1)) {
                    indexp1 = i;
                    break;
                }
            }
            if (indexp1 == -1) {
                // P1 does not belong to convex vertices, cannot compute diffraction
                // TODO handle concave path
                return new ArrayList<>();
            }
            // Transform array to set p1 at index=0
            Coordinate[] coordinatesShifted = new Coordinate[coordinates.length];
            // Copy from P1 to end in beginning of new array
            int len = (coordinates.length - 1) - indexp1;
            System.arraycopy(coordinates, indexp1, coordinatesShifted, 0, len);
            // Copy from 0 to P1 in the end of array
            System.arraycopy(coordinates, 0, coordinatesShifted, len, coordinates.length - len - 1);
            coordinatesShifted[coordinatesShifted.length - 1] = coordinatesShifted[0];
            coordinates = coordinatesShifted;
            indexp1 = 0;
            indexp2 = -1;
            for (int i = 1; i < coordinates.length - 1; i++) {
                if (coordinates[i].equals(p2)) {
                    indexp2 = i;
                    break;
                }
            }
            if (indexp2 == -1) {
                // P2 does not belong to convex vertices, cannot compute diffraction
                // TODO handle concave path
                return new ArrayList<>();
            }
            for (k = 0; k < coordinates.length - 1; k++) {
                LineSegment freeFieldTestSegment = new LineSegment(coordinates[k], coordinates[k + 1]);
                // Ignore intersection if iterating over other side (not parts of what is returned)
                if (left && k < indexp2 || !left && k >= indexp2) {
                    if (!freeFieldSegments.contains(freeFieldTestSegment)) {
                        // Check if we still are in the propagation domain
                        if(data.freeFieldFinder.getTriangleIdByCoordinate(coordinates[k]) == -1 ||
                                data.freeFieldFinder.getTriangleIdByCoordinate(coordinates[k+1]) ==-1) {
                            // This side goes over propagation path
                            return new ArrayList<>();
                        }
                        HashSet<Integer> buildingsOnPath2 = getBuildingsOnPath(coordinates[k], coordinates[k + 1]);
                        for (int i : buildingsOnPath2) {
                            if (!buildingsOnPath.contains(i)) {
                                List<Coordinate> roofPoints = data.freeFieldFinder.getWideAnglePointsByBuilding(i, 0, 2 * Math.PI);
                                roofPoints = cutRoofPointsWithPlane(cutPlane, roofPoints);
                                if (!roofPoints.isEmpty()) {
                                    convexHullIntersects = true;
                                    input.addAll(roofPoints.subList(0, roofPoints.size() - 1));
                                }
                                buildingsOnPath.add(i);
                            }
                        }
                        if (!convexHullIntersects) {
                            freeFieldSegments.add(freeFieldTestSegment);
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        if(left) {
            return Arrays.asList(Arrays.copyOfRange(coordinates,indexp1, indexp2 + 1));
        } else {
            ArrayList<Coordinate> inversePath = new ArrayList<>();
            inversePath.addAll(Arrays.asList(Arrays.copyOfRange(coordinates,indexp2, coordinates.length)));
            Collections.reverse(inversePath);
            return inversePath;
        }
    }

    public List<List<Coordinate>> computeVerticalEdgeDiffraction(Coordinate p1,
                                                                 Coordinate p2, List<PropagationDebugInfo> debugInfo) {
        List<List<Coordinate>> paths = new ArrayList<>();
        List<Coordinate> p1leftp2 = computeSideHull(true, p1, p2);
        if(!p1leftp2.isEmpty()) {
            paths.add(p1leftp2);
        }
        List<Coordinate> p1rightp2 = computeSideHull(false, p1, p2);
        if(!p1rightp2.isEmpty()) {
            paths.add(p1rightp2);
        }
        return paths;
    }

    /**
     * Compute project Z coordinate between p0 p1 of x,y.
     *
     * @param coordinateWithoutZ coordinate to set the Z value from Z interpolation of line
     * @param line               Extract Z values of this segment
     * @return coordinateWithoutZ with Z value computed from line.
     */
    private static Coordinate getProjectedZCoordinate(Coordinate coordinateWithoutZ, LineSegment line) {
        // Z value is the interpolation of source-receiver line
        return new Coordinate(coordinateWithoutZ.x, coordinateWithoutZ.y, Vertex.interpolateZ(
                line.closestPoint(coordinateWithoutZ), line.p0, line.p1));
    }


    private int nextFreeFieldNode(List<Coordinate> nodes, Coordinate startPt, LineSegment segmentConstraint,
                                  List<Integer> NodeExceptions, int firstTestNode,
                                  FastObstructionTest freeFieldFinder) {
        int validNode = firstTestNode;
        while (NodeExceptions.contains(validNode)
                || (validNode < nodes.size() && (Math.abs(segmentConstraint.projectionFactor(nodes.get(validNode))) > 1 || !freeFieldFinder.isFreeField(
                startPt, getProjectedZCoordinate(nodes.get(validNode), segmentConstraint))))) {
            validNode++;
        }
        if (validNode >= nodes.size()) {
            return -1;
        }
        return validNode;
    }


    private boolean[] findBuildingOnPath(Coordinate srcCoord,
                                         Coordinate receiverCoord, boolean vertivalDiffraction) {

        boolean somethingHideReceiver = false;
        boolean buildingOnPath = false;
        boolean[] somethingOnPath = new boolean[2];
        if (!vertivalDiffraction || !data.freeFieldFinder.isHasBuildingWithHeight()) {
            somethingHideReceiver = !data.freeFieldFinder.isFreeField(receiverCoord, srcCoord);
        } else {
            List<TriIdWithIntersection> propagationPath = new ArrayList<>();
            if (!data.freeFieldFinder.computePropagationPath(receiverCoord, srcCoord, false, propagationPath, false)) {
                // Propagation path not found, there is not direct field
                somethingHideReceiver = true;
            } else {
                if (!propagationPath.isEmpty()) {
                    for (TriIdWithIntersection inter : propagationPath) {
                        if (inter.isIntersectionOnBuilding() || inter.isIntersectionOnTopography()) {
                            somethingHideReceiver = true;
                        }
                        if (inter.getBuildingId() != 0) {
                            buildingOnPath = true;
                        }
                    }
                }
            }
        }
        somethingOnPath[0] = somethingHideReceiver;
        somethingOnPath[1] = buildingOnPath;
        return somethingOnPath;
    }

    List<PropagationPath> directPath(Coordinate srcCoord,
                                             Coordinate receiverCoord, boolean verticalDiffraction,boolean horizontalDiffraction,  List<PropagationDebugInfo> debugInfo) {


        List<PropagationPath> propagationPaths = new ArrayList<>();

        // Then, check if the source is visible from the receiver (not
        // hidden by a building)
        // Create the direct Line

        boolean freefield = true;
        boolean topographyHideReceiver = false;
        boolean buildingOnPath = false;

        List<TriIdWithIntersection> inters = new ArrayList<>();
        data.freeFieldFinder.computePropagationPath(srcCoord, receiverCoord, false, inters, true);
        for(TriIdWithIntersection intersection : inters) {
            if(intersection.getBuildingId() > 0) {
                topographyHideReceiver = true;
                buildingOnPath = true;
            }
            if(intersection.isIntersectionOnBuilding() || intersection.isIntersectionOnTopography()) {
                freefield = false;
                if(intersection.isIntersectionOnTopography()) {
                    topographyHideReceiver = true;
                }
            }
        }

        // double fav_probability = favrose[(int) (Math.round(calcRotationAngleInDegrees(srcCoord, receiverCoord) / 30))];

        if (!topographyHideReceiver && !buildingOnPath) {
            PropagationPath propagationPath = computeFreefield(receiverCoord, srcCoord,inters, debugInfo);
            propagationPaths.add(propagationPath);
        }

        //Process diffraction 3D
        // todo include rayleigh criterium
        if (verticalDiffraction && buildingOnPath && !freefield) {
            PropagationPath propagationPath3 = computeFreefield(receiverCoord, srcCoord, inters, debugInfo);
            PropagationPath propagationPath = computeHorizontalEdgeDiffraction(topographyHideReceiver, receiverCoord, srcCoord, inters, debugInfo);
            propagationPath.getSRList().addAll(propagationPath3.getSRList());
            propagationPaths.add(propagationPath);


        }

        if (topographyHideReceiver && data.isComputeHorizontalDiffraction() && horizontalDiffraction ) {
            // todo if one of the points > roof or < floor, get out this path
            PropagationPath propagationPath = new PropagationPath();
            PropagationPath propagationPath2 = new PropagationPath();

            // Left hand

            //List<List<Coordinate>> diffractedPaths = computeVerticalEdgeDiffraction(srcCoord, receiverCoord, debugInfo);
            List<Coordinate> coordinates = computeSideHull(true, srcCoord, receiverCoord);
            if(!coordinates.isEmpty()) {
                if (coordinates.size() > 2) {

                    propagationPath = computeFreefield(coordinates.get(1), coordinates.get(0),null, debugInfo);
                    propagationPath.getPointList().get(1).setType(PropagationPath.PointPath.POINT_TYPE.DIFV);

                    propagationPath2 = propagationPath;
                    int j;
                    for (j = 1; j < coordinates.size() - 2; j++) {
                        propagationPath = computeFreefield(coordinates.get(j + 1), coordinates.get(j),null, debugInfo);
                        propagationPath.getPointList().get(1).setType(PropagationPath.PointPath.POINT_TYPE.DIFV);
                        propagationPath2.getPointList().add(propagationPath.getPointList().get(1));
                        propagationPath2.getSegmentList().addAll(propagationPath.getSegmentList());
                    }
                    propagationPath = computeFreefield(coordinates.get(j + 1), coordinates.get(j),null, debugInfo);
                    propagationPath2.getPointList().add(propagationPath.getPointList().get(1));
                    propagationPath2.getSegmentList().addAll(propagationPath.getSegmentList());
                    propagationPaths.add(propagationPath2);
                }
            }

            // Right hand
            coordinates = computeSideHull(false, srcCoord, receiverCoord);
            if(!coordinates.isEmpty()) {
                if (coordinates.size() > 2) {
                    Collections.reverse(coordinates);
                    propagationPath = computeFreefield(coordinates.get(1), coordinates.get(0),null, debugInfo);
                    propagationPath.getPointList().get(1).setType(PropagationPath.PointPath.POINT_TYPE.DIFV);
                    propagationPath2 = propagationPath;
                    int j;
                    for (j = 1; j < coordinates.size() - 2; j++) {
                        propagationPath = computeFreefield(coordinates.get(j + 1), coordinates.get(j),null, debugInfo);
                        propagationPath.getPointList().get(1).setType(PropagationPath.PointPath.POINT_TYPE.DIFV);
                        propagationPath2.getPointList().add(propagationPath.getPointList().get(1));
                        propagationPath2.getSegmentList().addAll(propagationPath.getSegmentList());
                    }
                    propagationPath = computeFreefield(coordinates.get(j + 1), coordinates.get(j),null, debugInfo);
                    propagationPath2.getPointList().add(propagationPath.getPointList().get(1));
                    propagationPath2.getSegmentList().addAll(propagationPath.getSegmentList());
                    propagationPaths.add(propagationPath2);

                }
            }
        }
        return propagationPaths;
    }

    /**
     * Source-Receiver Direct+Reflection+Diffraction computation
     * @param srcCoord coordinate of source
     * @param srcId Source identifier
     * @param sourceLi Coefficient of power per meter for this point source
     * @param receiverCoord coordinate of receiver
     * @param rcvId receiver identifier
     * @param nearBuildingsWalls Walls to use in reflection
     * @param debugInfo
     * @param dataOut
     * @return Minimal power level (dB) or maximum attenuation (dB)
     */
    private double[] receiverSourcePropa(Coordinate srcCoord, int srcId, double sourceLi,
                                     Coordinate receiverCoord, int rcvId,
                                     List<FastObstructionTest.Wall> nearBuildingsWalls, List<PropagationDebugInfo> debugInfo, IComputeRaysOut dataOut) {

        List<PropagationPath> propagationPaths;
        // Build mirrored receiver list from wall list

        double PropaDistance = srcCoord.distance(receiverCoord);
        if (PropaDistance < data.maxSrcDist) {

            // Process direct path (including horizontal and vertical diffractions)
            propagationPaths = directPath(srcCoord, receiverCoord, data.isComputeVerticalDiffraction(), true, debugInfo);

            // Process specular reflection
            if (data.reflexionOrder > 0) {
                List<PropagationPath> propagationPaths_all = computeReflexion(receiverCoord, srcCoord, false, nearBuildingsWalls, debugInfo);
                propagationPaths.addAll(propagationPaths_all);
            }

            if (propagationPaths.size() > 0) {
                for (PropagationPath propagationPath : propagationPaths) {
                    propagationPath.idSource = srcId;
                    propagationPath.idReceiver = rcvId;
                }
                return dataOut.addPropagationPaths(srcId, sourceLi, rcvId, propagationPaths);
            }
        }
        return new double[0];
    }

    private static double insertPtSource(Coordinate receiverPos, Coordinate ptpos, double[] wj, double li, Integer sourceId, List<SourcePointInfo> sourceList) {
        // Compute maximal power at freefield at the receiver position with reflective ground
        double aDiv = -EvaluateAttenuationCnossos.getADiv(CGAlgorithms3D.distance(receiverPos, ptpos));
        double[] srcWJ = new double[wj.length];
        for(int idFreq = 0; idFreq < srcWJ.length; idFreq++) {
            srcWJ[idFreq] = wj[idFreq] * li * dbaToW(aDiv) * dbaToW(3);
        }
        sourceList.add(new SourcePointInfo(srcWJ, sourceId, ptpos, li));
        return ComputeRays.sumArray(srcWJ.length, srcWJ);
    }

    private double addLineSource(LineString source, Coordinate receiverCoord, int srcIndex, List<SourcePointInfo> sourceList, double[] wj) {
        double totalPowerRemaining = 0;
        ArrayList<Coordinate> pts = new ArrayList<Coordinate>();
        // Compute li to equation 4.1 NMPB 2008 (June 2009)
        Coordinate nearestPoint = JTSUtility.getNearestPoint(receiverCoord, source);
        double segmentSizeConstraint = Math.max(1, receiverCoord.distance3D(nearestPoint) / 2.0);
        double li = splitLineStringIntoPoints(source, segmentSizeConstraint, pts);
        for (Coordinate pt : pts) {
            totalPowerRemaining += insertPtSource(receiverCoord, pt, wj, li, srcIndex, sourceList);
        }
        return totalPowerRemaining;
    }

    /**
     * Compute sound level by frequency band at this receiver position
     *
     * @param receiverCoord
     */
    public void computeRaysAtPosition(Coordinate receiverCoord, int idReceiver, List<PropagationDebugInfo> debugInfo, IComputeRaysOut dataOut) {
        // List of walls within maxReceiverSource distance
        HashSet<Integer> processedLineSources = new HashSet<Integer>(); //Already processed Raw source (line and/or points)
        Set<FastObstructionTest.Wall> wallsReceiver = new HashSet<>();
        if (data.reflexionOrder > 0) {
            wallsReceiver.addAll(data.freeFieldFinder.getLimitsInRange(
                    data.maxRefDist, receiverCoord, false));
        }
        double searchSourceDistance = data.maxSrcDist;
        Envelope receiverSourceRegion = new Envelope(receiverCoord.x
                - searchSourceDistance, receiverCoord.x + searchSourceDistance,
                receiverCoord.y - searchSourceDistance, receiverCoord.y
                + searchSourceDistance
        );
        Iterator<Integer> regionSourcesLst = data.sourcesIndex
                .query(receiverSourceRegion);
        List<SourcePointInfo> sourceList = new ArrayList<>();
        // Sum of all sources power using only geometric dispersion with direct field
        double totalPowerRemaining = 0;
        while (regionSourcesLst.hasNext()) {
            Integer srcIndex = regionSourcesLst.next();
            if (!processedLineSources.contains(srcIndex)) {
                processedLineSources.add(srcIndex);
                Geometry source = data.sourceGeometries.get(srcIndex);
                double[] wj = data.getMaximalSourcePower(srcIndex);
                if (source instanceof Point) {
                    Coordinate ptpos = source.getCoordinate();
                    totalPowerRemaining += insertPtSource(receiverCoord, ptpos, wj, 1., srcIndex, sourceList);
                } else if (source instanceof LineString){
                    // Discretization of line into multiple point
                    // First point is the closest point of the LineString from
                    // the receiver
                    totalPowerRemaining += addLineSource((LineString)source, receiverCoord,srcIndex, sourceList, wj);
                } else if(source instanceof MultiLineString) {
                    for(int id = 0; id < source.getNumGeometries(); id++) {
                        Geometry subGeom = source.getGeometryN(id);
                        if(subGeom instanceof LineString) {
                            totalPowerRemaining += addLineSource((LineString) subGeom, receiverCoord, srcIndex, sourceList, wj);
                        }
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Sound source %s geometry are not supported", source.getGeometryType()));
                }
            }
        }
        // Sort sources by power contribution descending
        Collections.sort(sourceList);
        double powerAtSource = 0;
        //Iterate over source point sorted by maximal power by descending order
        for (SourcePointInfo src : sourceList) {
            // For each Pt Source - Pt Receiver
            Coordinate srcCoord = src.position;

            Set<FastObstructionTest.Wall> wallsSource = new HashSet<>(wallsReceiver);
            if (data.reflexionOrder > 0) {
                wallsSource.addAll(data.freeFieldFinder.getLimitsInRange(
                        data.maxRefDist, srcCoord, false));
            }
            double[] power = receiverSourcePropa(srcCoord, src.sourcePrimaryKey, src.li, receiverCoord, idReceiver,
                    new ArrayList<>(wallsSource), debugInfo, dataOut);
            double global = ComputeRays.sumArray(power.length, ComputeRays.dbaToW(power));
            totalPowerRemaining -= src.globalWj;
            if(power.length > 0) {
                powerAtSource += global;
            } else {
                powerAtSource += src.globalWj;
            }
            totalPowerRemaining = Math.max(0, totalPowerRemaining);
            // If the delta between already received power and maximal potential power received is inferior than than data.maximumError
            if (data.maximumError > 0 && wToDba(powerAtSource + totalPowerRemaining) - wToDba(powerAtSource) < data.maximumError) {
                break; //Stop looking for more rays
            }
        }
        // No more rays for this receiver
        dataOut.finalizeReceiver(idReceiver);
    }

    /**
     * Must be called before computeSoundLevelAtPosition
     */
    public void initStructures() {
        //Build R-tree for soil geometry and soil type
        rTreeOfGeoSoil = new STRtree();
        List<GeoWithSoilType> soilTypeList = data.getSoilList();
        if (soilTypeList != null) {
            for (int i = 0; i < soilTypeList.size(); i++) {
                GeoWithSoilType geoWithSoilType = soilTypeList.get(i);
                rTreeOfGeoSoil.insert(geoWithSoilType.getGeo().getEnvelopeInternal(),
                        new EnvelopeWithIndex<Integer>(geoWithSoilType.getGeo().getEnvelopeInternal(), i));
            }
        }
    }

    public void runDebug(IComputeRaysOut computeRaysOut, List<PropagationDebugInfo> debugInfo) {

        initStructures();

        // Computed sound level of vertices
        //dataOut.setVerticesSoundLevel(new double[data.receivers.size()]);

        // For each vertices, find sources where the distance is within
        // maxSrcDist meters
        ProgressVisitor propaProcessProgression = data.cellProg;

        int splitCount = threadCount;
        ThreadPool threadManager = new ThreadPool(
                splitCount,
                splitCount + 1, Long.MAX_VALUE,
                TimeUnit.SECONDS);
        int maximumReceiverBatch = (int) Math.ceil(data.receivers.size() / (double) splitCount);
        int endReceiverRange = 0;
        while (endReceiverRange < data.receivers.size()) {
            int newEndReceiver = Math.min(endReceiverRange + maximumReceiverBatch, data.receivers.size());
            RangeReceiversComputation batchThread = new RangeReceiversComputation(endReceiverRange,
                    newEndReceiver, this, debugInfo, propaProcessProgression,
                    computeRaysOut.subProcess(endReceiverRange ,newEndReceiver));
            if(threadCount != 1) {
                threadManager.executeBlocking(batchThread);
            } else {
                batchThread.run();
            }
            endReceiverRange = newEndReceiver;
        }
        threadManager.shutdown();
        try {
            threadManager.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
        }
    }

    public void run(IComputeRaysOut computeRaysOut) {
        runDebug(computeRaysOut, null);
    }


    private double getIntersectedDistance(Geometry geo) {

        double totDistance = 0.;
        for (int i = 0; i < geo.getNumGeometries(); i++) {
            Coordinate[] coordinates = geo.getGeometryN(i).getCoordinates();
            if (coordinates.length > 1 && geo.getGeometryN(i) instanceof LineString) {
                totDistance += geo.getGeometryN(i).getLength();
            }
        }
        return totDistance;

    }

    private static class RangeReceiversComputation implements Runnable {
        private final int startReceiver; // Included
        private final int endReceiver; // Excluded
        private ComputeRays propagationProcess;
        private List<PropagationDebugInfo> debugInfo;
        private ProgressVisitor progressVisitor;
        private IComputeRaysOut dataOut;

        public RangeReceiversComputation(int startReceiver, int endReceiver, ComputeRays propagationProcess,
                                         List<PropagationDebugInfo> debugInfo, ProgressVisitor progressVisitor,
                                         IComputeRaysOut dataOut) {
            this.startReceiver = startReceiver;
            this.endReceiver = endReceiver;
            this.propagationProcess = propagationProcess;
            this.debugInfo = debugInfo;
            this.progressVisitor = progressVisitor;
            this.dataOut = dataOut;
        }

        @Override
        public void run() {

            for (int idReceiver = startReceiver; idReceiver < endReceiver; idReceiver++) {
                Coordinate receiverCoord = propagationProcess.data.receivers.get(idReceiver);

                propagationProcess.computeRaysAtPosition(receiverCoord, idReceiver, debugInfo, dataOut);

                if(progressVisitor != null) {
                    progressVisitor.endStep();
                }
            }
        }
    }

    /**
     * Offset de Z coordinates by the height of the ground
     */
    public static final class AbsoluteCoordinateSequenceFilter implements CoordinateSequenceFilter {
        AtomicBoolean geometryChanged = new AtomicBoolean(false);
        FastObstructionTest fastObstructionTest;
        boolean resetZ;

        /**
         * Constructor
         *
         * @param fastObstructionTest Initialised instance of fastObstructionTest
         * @param resetZ              If filtered geometry contain Z and resetZ is false, do not update Z.
         */
        public AbsoluteCoordinateSequenceFilter(FastObstructionTest fastObstructionTest, boolean resetZ) {
            this.fastObstructionTest = fastObstructionTest;
            this.resetZ = resetZ;
        }

        @Override
        public void filter(CoordinateSequence coordinateSequence, int i) {
            Coordinate pt = coordinateSequence.getCoordinate(i);
            Double zGround = fastObstructionTest.getHeightAtPosition(pt);
            if (!zGround.isNaN() && (resetZ || Double.isNaN(pt.getOrdinate(2)) || Double.compare(0, pt.getOrdinate(2)) == 0)) {
                pt.setOrdinate(2, zGround + (Double.isNaN(pt.getOrdinate(2)) ? 0 : pt.getOrdinate(2)));
                geometryChanged.set(true);
            }
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return geometryChanged.get();
        }
    }

    private static final class SourcePointInfo implements Comparable<SourcePointInfo> {
        private double[] wj;
        private double li; //
        private int sourcePrimaryKey;
        private Coordinate position;
        private double globalWj;

        /**
         * @param wj Maximum received power from this source
         * @param sourcePrimaryKey
         * @param position
         */
        public SourcePointInfo(double[] wj, int sourcePrimaryKey, Coordinate position, double li) {
            this.wj = wj;
            this.sourcePrimaryKey = sourcePrimaryKey;
            this.position = position;
            this.globalWj = ComputeRays.sumArray(wj.length, wj);
            this.li = li;
        }

        /**
         * @return coefficient to apply to linear source as sound power per meter length
         */
        public double getLi() {
            return li;
        }

        public double[] getWj() {
            return wj;
        }

        public void setWj(double[] wj) {
            this.wj = wj;
            this.globalWj = ComputeRays.sumArray(wj.length, wj);
        }

        @Override
        public int compareTo(SourcePointInfo sourcePointInfo) {
            int cmp = -Double.compare(globalWj, sourcePointInfo.globalWj);
            if(cmp == 0) {
                return Integer.compare(sourcePrimaryKey, sourcePointInfo.sourcePrimaryKey);
            } else {
                return cmp;
            }
        }
    }
}
