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

package org.noise_planet.noisemodelling.wps


import org.junit.Test
import org.noise_planet.noisemodelling.wps.Database_Manager.Display_Database
import org.noise_planet.noisemodelling.wps.Experimental.Cars_Dynamic_map
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_Symuvia
import org.noise_planet.noisemodelling.wps.NoiseModelling.Traffic_Probabilistic_Modelling
import org.noise_planet.noisemodelling.wps.Import_and_Export.Import_OSM
import org.noise_planet.noisemodelling.wps.Receivers.Building_Grid

class TestDynamicTools extends JdbcTestCase  {

    @Test
    void testRoadProba() {


        // Import OSM file
        String res = new Import_OSM().exec(connection,
                ["pathFile": TestTutorials.getResource("map.osm.gz").getPath(),
                 "targetSRID" : 2154])

        new Building_Grid().exec(connection,  ["tableBuilding"   : "BUILDINGS",
                                               "delta"           : 10,
                                               "height"          : 4,
                                               "sourcesTableName": "ROADS",
                                               "fenceTableName"  : "BUILDINGS"])

        // Check database
        res = new Display_Database().exec(connection, [])

        assertTrue(res.contains("GROUND"))
        assertTrue(res.contains("BUILDINGS"))
        assertTrue(res.contains("ROADS"))

        res = new Traffic_Probabilistic_Modelling().exec(connection, ["tableRoads"  : "ROADS",
                                                                      "tableBuilding" : "BUILDINGS",
                                                                      "nIterations" : 10,
                                                                      "tableReceivers": "RECEIVERS"])

        // Check database
        res = new Display_Database().exec(connection, [])

        assertTrue(res.contains("L_PROBA"))

    }


    @Test
    void testRoadDyn() {

        // Check empty database
        Object res = new Display_Database().exec(connection, [])

        assertEquals("", res)
        // Import OSM file
        res = new Import_Symuvia().exec(connection,
                ["pathFile": TestImportExport.getResource("Symuvia/symuvia.xml").getPath(),
                 "tini": 20 ,
                 "tend":  25 ,
                 "defaultSRID" : 2154])

        res = new Display_Database().exec(connection, [])

        res = new Import_File().exec(connection,
                ["pathFile" : TestImportExport.getResource("Symuvia/Receivers.shp").getPath(),
                 "inputSRID": "2154"])

        res = new Import_File().exec(connection,
                ["pathFile" : TestImportExport.getResource("Symuvia/ROADS.shp").getPath(),
                 "inputSRID": "2154"])

        res = new Import_File().exec(connection,
                ["pathFile" : TestImportExport.getResource("Symuvia/BUILDINGS_OSM.shp").getPath(),
                 "inputSRID": "2154"])

        res = new Import_File().exec(connection,
                ["pathFile" : TestImportExport.getResource("Symuvia/SOURCES_POSITIONS.shp").getPath(),
                 "inputSRID": "2154"])

        res = new Display_Database().exec(connection, [])

        assertTrue(res.contains("SYMUVIA_TRAJ"))

        res = new Cars_Dynamic_map().exec(connection, ["sourcesPositionTableName"    : "SOURCES_POSITIONS",
                                                       "buildingTableName" : "BUILDINGS_OSM",
                                                       "sourcesTimeTableName"   : "SYMUVIA_TRAJ_20_25",
                                                       "tini"   : 20,
                                                       "tend"   : 25,
                                                       "receiversTableName": "RECEIVERS"])



        // Check database
        res = new Display_Database().exec(connection, [])

        assertTrue(res.contains("L_PROBA"))

    }


}
