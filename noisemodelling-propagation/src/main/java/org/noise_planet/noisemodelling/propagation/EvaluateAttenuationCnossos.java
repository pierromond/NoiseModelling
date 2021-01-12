/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */


package org.noise_planet.noisemodelling.propagation;

import org.noise_planet.noisemodelling.pathfinder.PropagationPath;
import org.noise_planet.noisemodelling.pathfinder.SegmentPath;

import java.util.List;

/**
 * Return the dB value corresponding to the parameters
 * Following Directive 2015/996/EN
 * https://circabc.europa.eu/sd/a/9566c5b9-8607-4118-8427-906dab7632e2/Directive_2015_996_EN.pdf
 * @author Pierre Aumond
 */

public class EvaluateAttenuationCnossos {
    private final static double ONETHIRD = 1. / 3.;
    private int nbfreq;
    private double[] freq_lambda;
    private double[] aGlobal;
    boolean gToSigma = false; // Todo publish parameter issue #13

    public void setaGlobal(double[] aGlobal) {
        this.aGlobal = aGlobal;
    }

    public void setFreq_lambda(double[] freq_lambda) {
        this.freq_lambda = freq_lambda;
    }

    public static double dbaToW(double dBA) {
        return Math.pow(10., dBA / 10.);
    }

    public static double wToDba(double w) {
        return 10 * Math.log10(w);
    }

    public double[] getaGlobal() {
        return aGlobal;
    }

    public boolean isgToSigma() {
        return gToSigma;
    }

    public void setgToSigma(boolean gToSigma) {
        this.gToSigma = gToSigma;
    }

    public double[] getDeltaDif(SegmentPath srpath, PropagationProcessPathData data) {
        double[] DeltaDif = new double[data.freq_lvl.size()];
        double cprime;

        for (int idfreq = 0; idfreq < data.freq_lvl.size(); idfreq++) {

            double Ch = 1;// Math.min(h0 * (data.celerity / freq_lambda[idfreq]) / 250, 1);

            if (srpath.eLength > 0.3) {
                double gammaPart = Math.pow((5 * freq_lambda[idfreq]) / srpath.eLength, 2);
                cprime = (1. + gammaPart) / (ONETHIRD + gammaPart);
            } else {
                cprime = 1.;
            }

            //(7.11) NMP2008 P.32
            double testForm = (40 / freq_lambda[idfreq])
                    * cprime * srpath.getDelta();

            double deltaDif = 0.;

            if (testForm >= -2.) {
                deltaDif = 10 * Ch * Math
                        .log10(Math.max(0, 3 + testForm));
            }

            DeltaDif[idfreq] = Math.max(0, deltaDif);

        }
        return DeltaDif;

    }


    /**
     * Compute attenuation of sound energy by distance. Minimum distance is one
     * meter.
     * @param distance Distance in meter
     * @return Attenuated sound level. Take only account of geometric dispersion
     * of sound wave.
     */
    public static double getADiv(double distance) {
        return wToDba(4 * Math.PI * Math.max(1, distance * distance));
    }

    /**
     * Compute the attenuation of atmospheric absorption
     * @param dist       Propagation distance
     * @param alpha_atmo Atmospheric alpha (dB/km)
     * @return
     */
    public static double getAAtm(double dist, double alpha_atmo) {
        return (alpha_atmo * dist) / 1000.;
    }

    /**
     *
     * @return
     */
    public static double[] getAGroundCore(PropagationPath path, SegmentPath segmentPath, PropagationProcessPathData data) {

        double[] aGround = new double[data.freq_lvl.size()];
        double aGroundmin;
        double AGround;

        for (int idfreq = 0; idfreq < data.freq_lvl.size(); idfreq++) {
            //NF S 31-133 page 41 c
            double k = 2 * Math.PI * data.freq_lvl.get(idfreq) / data.getCelerity();
            //NF S 31-113 page 41 w
            double w = 0.0185 * Math.pow(data.freq_lvl.get(idfreq), 2.5) * Math.pow(segmentPath.gw, 2.6) /
                    (Math.pow(data.freq_lvl.get(idfreq), 1.5) * Math.pow(segmentPath.gw, 2.6) + 1.3 * Math.pow(10, 3) * Math.pow(data.freq_lvl.get(idfreq), 0.75) * Math.pow(segmentPath.gw, 1.3) + 1.16 * Math.pow(10, 6));
            //NF S 31-113 page 41 Cf
            double cf = segmentPath.dp * (1 + 3 * w * segmentPath.dp * Math.pow(Math.E, -Math.sqrt(w * segmentPath.dp))) / (1 + w * segmentPath.dp);
            //NF S 31-113 page 41 A sol

            if (path.isFavorable()) {
                if (data.isPrime2520()) {
                    if (segmentPath.testFormPrime <= 1) {
                        aGroundmin = -3 * (1 - segmentPath.gm);
                    } else {
                        aGroundmin = -3 * (1 - segmentPath.gm) * (1 + 2 * (1 - (1 / segmentPath.testFormPrime)));
                    }
                } else {
                    if (segmentPath.testForm <= 1) {
                        aGroundmin = -3 * (1 - segmentPath.gm);
                    } else {
                        aGroundmin = -3 * (1 - segmentPath.gm) * (1 + 2 * (1 - (1 / segmentPath.testForm)));
                    }
                }
                /** eq. 2.5.19**/
                AGround = -10 * Math.log10(4 * Math.pow(k, 2) / Math.pow(segmentPath.dp, 2) *
                        (Math.pow(segmentPath.zsPrime, 2) - Math.sqrt(2 * cf / k) * segmentPath.zsPrime + cf / k) *
                        (Math.pow(segmentPath.zrPrime, 2) - Math.sqrt(2 * cf / k) * segmentPath.zrPrime + cf / k));
            } else {
                /** eq. 2.5.15**/
                AGround = -10 * Math.log10(4 * Math.pow(k, 2) / Math.pow(segmentPath.dp, 2) *
                        (Math.pow(segmentPath.zs, 2) - Math.sqrt(2 * cf / k) * segmentPath.zs + cf / k) *
                        (Math.pow(segmentPath.zr, 2) - Math.sqrt(2 * cf / k) * segmentPath.zr + cf / k));
                /** eq. 2.5.18**/
                aGroundmin = -3 * (1 - segmentPath.gm);
            }
            aGround[idfreq] = Math.max(AGround, aGroundmin);

        }
        return aGround;
    }

    /**
     * Formulae Eq. 2.5.31 - Eq. 2.5.32
     * @param aGround        Asol(O,R) or Asol(S,O) (sol mean ground)
     * @param deltaDifPrim Δdif(S,R') if Asol(S,O) is given or Δdif(S', R) if Asol(O,R)
     * @param deltaDif     Δdif(S, R)
     * @return Δsol(S, O) if Asol(S,O) is given or Δsol(O,R) if Asol(O,R) is given
     */
    public double getDeltaGround(double aGround, double deltaDifPrim, double deltaDif) {
        double attArg = 1 + (Math.pow(10, -aGround / 20) - 1) * Math.pow(10, -(deltaDifPrim - deltaDif) / 20);
        if (attArg < 0) {
            attArg = 0;
        }
        return -20 * Math.log10(attArg);
    }


    public double[] getARef(PropagationPath path, PropagationProcessPathData data) {
        double[] aRef = new double[data.freq_lvl.size()];
        for (int idf = 0; idf < nbfreq; idf++) {
            for (int idRef = 0; idRef < path.refPoints.size(); idRef++) {
                List<Double> alpha = path.getPointList().get(path.refPoints.get(idRef)).alphaWall;
                /*if (gToSigma || alphaUniqueValue > 1){
                    PropagationProcessData.getWallAlpha(alphaUniqueValue, data.freq_lvl.get(idf));
                }*/
                aRef[idf] += -10 * Math.log10(1 - alpha.get(idf));
            }
        }
        return aRef;
    }


    public double[] getAGround(SegmentPath segmentPath, PropagationPath path, PropagationProcessPathData data) {
        double[] aGround = new double[data.freq_lvl.size()];
        double aGroundmin;

        // Here there is a debate if use this condition or not
        if (segmentPath.gPath == 0 && data.isgDisc()) {
            if (path.isFavorable()) {
                if (segmentPath.testForm <= 1) {
                    aGroundmin = -3 * (1 - segmentPath.gm);
                } else {
                    aGroundmin = -3 * (1 - segmentPath.gm) * (1 + 2 * (1 - (1 / segmentPath.testForm)));
                }
            } else {
                aGroundmin = -3;
            }
            java.util.Arrays.fill(aGround, aGroundmin);
        } else {
            aGround = getAGroundCore(path, segmentPath, data);
        }
        return aGround;
    }

    /**
     *
     * @param path
     * @param data
     * @return
     */
    public double[] getABoundary(PropagationPath path, PropagationProcessPathData data) {

        List<SegmentPath> srPath = path.getSRList();

        double[] aGround;
        double[] aBoundary;
        double[] aDif = new double[data.freq_lvl.size()];

        // Set Gm and Gw for AGround SR - Table 2.5.b
        if (path.isFavorable()) {
            srPath.get(0).setGw(srPath.get(0).gPath);
            srPath.get(0).setGm(srPath.get(0).gPathPrime);
        } else {
            srPath.get(0).setGw(srPath.get(0).gPathPrime);
            srPath.get(0).setGm(srPath.get(0).gPathPrime);
        }

        aGround = getAGround(srPath.get(0), path, data);
        aBoundary = aGround;
        if (path.difVPoints.size() > 0) {
            List<SegmentPath> segmentPath = path.getSegmentList();
            double[] DeltaDifSR;
            DeltaDifSR = getDeltaDif(srPath.get(0), data);
            aDif = DeltaDifSR;
            // Eq 2.5.30 - Eq. 2.5.31 - Eq. 2.5.32
            for (int idf = 0; idf < nbfreq; idf++) {
                aBoundary[idf] = aDif[idf] + aGround[idf];
            }

        }
        if (path.difHPoints.size() > 0) {
            List<SegmentPath> segmentPath = path.getSegmentList();

            double[] DeltaDifSR;
            double[] DeltaDifSpR;
            double[] DeltaDifSRp;
            double[] aGroundSO;
            double[] aGroundOR;

            DeltaDifSR = getDeltaDif(srPath.get(0), data);
            DeltaDifSpR = getDeltaDif(srPath.get(srPath.size() - 2), data);
            DeltaDifSRp = getDeltaDif(srPath.get(srPath.size() - 1), data);

            // Set Gm and Gw for AGround SO - Table 2.5.b
            if (path.isFavorable()) {
                segmentPath.get(0).setGw(segmentPath.get(0).gPath);
                segmentPath.get(0).setGm(segmentPath.get(0).gPathPrime);
            } else {
                segmentPath.get(0).setGw(segmentPath.get(0).gPathPrime);
                segmentPath.get(0).setGm(segmentPath.get(0).gPathPrime);
            }

            // TODO Should be Z o,s' but can't find how to compute this
            aGroundSO = getAGround(segmentPath.get(0), path, data);

            // Set Gm and Gw for AGround OR - Table 2.5.b
            if (path.isFavorable()) {
                segmentPath.get(segmentPath.size() - 1).setGw(segmentPath.get(segmentPath.size() - 1).gPath);
                segmentPath.get(segmentPath.size() - 1).setGm(segmentPath.get(segmentPath.size() - 1).gPath);
            } else {
                segmentPath.get(segmentPath.size() - 1).setGw(segmentPath.get(segmentPath.size() - 1).gPath);
                segmentPath.get(segmentPath.size() - 1).setGm(segmentPath.get(segmentPath.size() - 1).gPath);
            }
            aGroundOR = getAGround(segmentPath.get(segmentPath.size() - 1), path, data);


            // Eq 2.5.30 - Eq. 2.5.31 - Eq. 2.5.32
            for (int idf = 0; idf < nbfreq; idf++) {
                // see 5.3 Equivalent heights from AFNOR document
                if (segmentPath.get(0).zs <= 0.0000001 || segmentPath.get(segmentPath.size() - 1).zr <= 0.0000001) {
                    aDif[idf] = Math.min(25, DeltaDifSR[idf]) + aGroundSO[idf] + aGroundOR[idf];
                } else {
                    aDif[idf] = Math.min(25, DeltaDifSR[idf]) + getDeltaGround(aGroundSO[idf], DeltaDifSpR[idf],
                            DeltaDifSR[idf]) + getDeltaGround(aGroundOR[idf], DeltaDifSRp[idf], DeltaDifSR[idf]);
                }

            }

            aBoundary = aDif;
        }

        return aBoundary;
    }

    public double[] evaluate(PropagationPath path, PropagationProcessPathData data) {
        // init
        aGlobal = new double[data.freq_lvl.size()];
        double[] aBoundary;
        double[] aRef;
        nbfreq = data.freq_lvl.size();

        // Init wave length for each frequency
        freq_lambda = new double[nbfreq];
        for (int idf = 0; idf < nbfreq; idf++) {
            if (data.freq_lvl.get(idf) > 0) {
                freq_lambda[idf] = data.getCelerity() / data.freq_lvl.get(idf);
            } else {
                freq_lambda[idf] = 1;
            }
        }

        // init evolved path
        path.initPropagationPath();

        // init atmosphere
        double[] alpha_atmo = data.getAlpha_atmo();

        double aDiv;
        // divergence
        if (path.refPoints.size() > 0) {
            aDiv = getADiv(path.getSRList().get(0).dPath);
        } else {
            aDiv = getADiv(path.getSRList().get(0).d);
        }


        // boundary (ground + diffration)
        aBoundary = getABoundary(path, data);

        // reflections
        aRef = getARef(path, data);

        for (int idfreq = 0; idfreq < nbfreq; idfreq++) {
            // atm
            double aAtm;
            if (path.difVPoints.size() > 0 || path.refPoints.size() > 0) {
                aAtm = getAAtm(path.getSRList().get(0).dPath, alpha_atmo[idfreq]);
            } else {
                aAtm = getAAtm(path.getSRList().get(0).d, alpha_atmo[idfreq]);
            }

            aGlobal[idfreq] = -(aDiv + aAtm + aBoundary[idfreq] + aRef[idfreq]);

        }
        return aGlobal;
    }
}
