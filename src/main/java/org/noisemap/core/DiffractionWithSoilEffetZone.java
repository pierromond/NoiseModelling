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
package org.noisemap.core;

import com.vividsolutions.jts.geom.LineString;
/**
 * DiffractionWithSoilEffetZone work for FastOBstructionTest, 
 * aims to keep the 3D diffraction, first diffraction zone and last diffraction zone data, 
 * to give them to propagation process data
 * @author SU Qi
 */
public class DiffractionWithSoilEffetZone {
           private Double[] diffractionData= new Double[3]; //3D diffraction data
           private LineString firstZone;//first zone 3D diffraction
           private LineString lastZone;//last zone 3D diffraction
           
           public DiffractionWithSoilEffetZone(Double[] diffractionData, LineString firstZone, LineString lastZone){
               this.diffractionData=diffractionData;
               this.firstZone=firstZone;
               this.lastZone=lastZone;
           }
           public Double[] getDiffractionData(){

               return this.diffractionData;
           }
           public LineString getFirstZone(){
               return this.firstZone;
           }

           public LineString getLastZone(){
               return this.lastZone;
           }
}
