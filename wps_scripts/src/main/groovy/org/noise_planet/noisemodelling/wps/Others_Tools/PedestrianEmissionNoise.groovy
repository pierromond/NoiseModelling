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

package org.noise_planet.noisemodelling.wps.Others_Tools

import geoserver.GeoServer
import geoserver.catalog.Store
import groovy.sql.Sql
import groovy.time.TimeCategory
import org.geotools.jdbc.JDBCDataStore
import org.h2gis.functions.io.osm.OSMRead
import org.h2gis.functions.spatial.crs.ST_SetSRID
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.TableLocation
import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

import java.sql.Connection
import java.sql.Statement

title = 'Pedestrian'
description = 'Pedestrian - Convert OSM/OSM.GZ file (https://www.openstreetmap.org) to input tables. ' +
        ' <br>Be careful, this treatment can be blocking if the table is large. Some bugs have also been detected for some specific areas.' +
        '<br> The user can choose to create one to three output tables : <br>' +
        '-  <b> BUILDINGS  </b> : a table containing the building. </br>' +
        '-  <b> GROUND  </b> : surface/ground acoustic absorption table. </br>' +
        '-  <b> ROADS  </b> : a table containing the roads. </br>'

inputs = [newSRID  : [name: 'Projection identifier', title: 'Projection identifier', description: 'New projection identifier (also called SRID) of your table. ' +
        'It should be an EPSG code, a integer with 4 or 5 digits (ex: 3857 is Web Mercator projection). ' +
        '</br>  All coordinates will be projected from the specified EPSG to WGS84 coordinates. ' +
        '</br> This entry is optional because many formats already include the projection and you can also import files without geometry attributes.',
                      type: Integer.class]]

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

// main function of the script
def exec(Connection connection, input) {

    // output string, the information given back to the user
    String resultString = ""

    // print to command window
    System.out.println('Start : Get Input Data from OSM')
    def start = new Date()

    // -------------------
    // Get every inputs
    // -------------------
    // Get new SRID
    Integer newSrid = input['newSRID'] as Integer


    // Create a sql connection to interact with the database in SQL
    Sql sql = new Sql(connection)
    connection = new ConnectionWrapper(connection)
    
        System.out.println('Pedestrian')
sql.execute('drop table pedestrian_walkways if exists;')
sql.execute("create table pedestrian_walkways as select ST_UNION(ST_ACCUM((ST_BUFFER(st_precisionreducer(the_geom,2), 5)))) the_geom from ROADS where ST_Length(THE_GEOM) < 2000 AND (TYPE = 'pedestrian' or TYPE = 'path' or TYPE =   'footway' or TYPE =  'crossing' or TYPE =  'steps');")



    System.out.println('pedestrian_walkways')

sql.execute("drop table Roads_large if exists;"+
"create table Roads_large(the_geom geometry, wb float, TYPE TEXT, PK float) as select st_precisionreducer(the_geom,2),"+
"CASEWHEN(TYPE='primary', 7,"+
"CASEWHEN(TYPE='primary_link', 7,"+
"CASEWHEN(TYPE='secondary',3,"+
"CASEWHEN(TYPE='secondary_link',3,"+
"CASEWHEN(TYPE='tertiary',1.75,"+
"CASEWHEN(TYPE='tertiary_link',1.75,"+
"CASEWHEN(TYPE='motorway',7,"+
"CASEWHEN(TYPE='motorway_link',7,"+
"CASEWHEN(TYPE='trunk',7,"+
"CASEWHEN(TYPE='trunk_link',7,"+
"CASEWHEN(TYPE='service',1.75,"+
"CASEWHEN(TYPE='cycleway',1.75,"+
"CASEWHEN(TYPE='living_street',1.75,"+
"CASEWHEN(TYPE='residential',3.5,"+
"CASEWHEN(TYPE='bus_guideway',1.75,"+
"CASEWHEN(TYPE='escape',1.75,"+
"CASEWHEN(TYPE='raceway',3.5,"+
"CASEWHEN(TYPE='road',1.75,"+
"CASEWHEN(TYPE='unclassified',3,0))))))))))))))))))), TYPE, PK FROM ROADS WHERE ST_Length(THE_GEOM) < 3000 ;")

   System.out.println('Roads_large')
sql.execute("drop table Roads_large_b if exists;"+
"create table Roads_large_b(the_geom geometry) as select st_precisionreducer(ST_UNION(ST_ACCUM(ST_BUFFER(the_geom, wb))),2) FROM Roads_large;"+

"drop table Roads_small if exists;"+
"create table Roads_small(the_geom geometry, lt float, TYPE TEXT, PK float) as select st_precisionreducer(the_geom,2),"+
"CASEWHEN(TYPE='primary', 3,"+
"CASEWHEN(TYPE='primary_link', 3,"+
"CASEWHEN(TYPE='secondary',3,"+
"CASEWHEN(TYPE='secondary_link',3,"+
"CASEWHEN(TYPE='tertiary',2,"+
"CASEWHEN(TYPE='tertiary_link',2,"+
"CASEWHEN(TYPE='motorway',3,"+
"CASEWHEN(TYPE='motorway_link',3,"+
"CASEWHEN(TYPE='trunk',3,"+
"CASEWHEN(TYPE='trunk_link',3,"+
"CASEWHEN(TYPE='service',2,"+
"CASEWHEN(TYPE='cycleway',2,"+
"CASEWHEN(TYPE='living_street',1,"+
"CASEWHEN(TYPE='residential',2.5,"+
"CASEWHEN(TYPE='bus_guideway',2,"+
"CASEWHEN(TYPE='escape',1.5,"+
"CASEWHEN(TYPE='raceway',1.5,"+
"CASEWHEN(TYPE='road',1.5,"+
"CASEWHEN(TYPE='unclassified',1.5,0))))))))))))))))))), TYPE, PK FROM ROADS WHERE ST_Length(THE_GEOM) < 3000 ;")

  System.out.println('Roads_small')
sql.execute("ALTER TABLE roads_large ALTER COLUMN PK SET NOT NULL;"+
"ALTER TABLE roads_large ADD PRIMARY KEY (PK) ;"+

"ALTER TABLE roads_small ALTER COLUMN PK SET NOT NULL;"+
"ALTER TABLE roads_small ADD PRIMARY KEY (PK) ;")

System.out.println('sidewalks_roads')

sql.execute("drop table sidewalks_roads if exists;"+
"create table sidewalks_roads(the_geom geometry) as select st_precisionreducer(ST_UNION(ST_ACCUM(ST_BUFFER(a.the_geom, a.wb+b.lt))),2) FROM roads_large a, roads_small b where a.PK=b.PK;")


   System.out.println('Spatial index')

sql.execute("CREATE SPATIAL INDEX ON sidewalks_roads(THE_GEOM);"+
"CREATE SPATIAL INDEX ON Roads_large_b(THE_GEOM);")

   System.out.println('Diff RoadsSidewalks index')

sql.execute("drop table sidewalks if exists;"+
"create table sidewalks as select ST_DIFFERENCE(b.the_geom, a.the_geom) the_geom from Roads_large_b a, sidewalks_roads b;")


System.out.println('parks')

sql.execute("drop table parks if exists;"+
"create table parks as select * from \"NATURAL\" where TYPE = 'park';")

System.out.println('sidewalks_walkways')
sql.execute("drop table sidewalks_walkways if exists;")
sql.execute("create table sidewalks_walkways as select * from pedestrian_walkways union select * from sidewalks ;")

//sql.execute("drop table PARKINGS if exists;")
//sql.execute("create table PARKINGS as select ST_UNION(ST_ACCUM(the_geom)) the_geom from ROADS where FCLASS <> 'parking_underground';")

//sql.execute("drop table roads_GROUP_BY_NAME if exists;")
//sql.execute("create table roads_GROUP_BY_NAME as select name, st_union(st_accum(the_geom)) the_geom from ROADS_NS2 group by name;")

//sql.execute("drop table PLACES_BEF if exists;")
//sql.execute("create table PLACES_BEF as select name, st_polygonize(the_geom) the_geom from roads_GROUP_BY_NAME where name<>'';")

//sql.execute("drop table PLACES if exists;")
//sql.execute("create table PLACES as select name, the_geom from PLACES_BEF where ST_GeometryType(the_geom)<>'null';")

//sql.execute("drop table sidewalks_walkways_places if exists;")
//sql.execute("create table sidewalks_walkways_places as select the_geom from sidewalks_walkways union select the_geom from PLACES;")

System.out.println('Union')
sql.execute("drop table WATERWAYS_UNION if exists;")
sql.execute("create table WATERWAYS_UNION as select st_union(st_accum(the_geom)) THE_GEOM from WATERWAYS a;")

sql.execute("drop table sidewalks_walkways_UNION if exists;")
sql.execute("create table sidewalks_walkways_UNION as select st_union(st_accum(the_geom)) THE_GEOM from sidewalks_walkways a;")


sql.execute("drop table BUILDINGS_UNION if exists;")
sql.execute("create table BUILDINGS_UNION as select st_union(st_accum(the_geom)) THE_GEOM from BUILDINGS a;")


System.out.println('Spatial index')
sql.execute("CREATE SPATIAL INDEX ON WATERWAYS_UNION(THE_GEOM);"+
"CREATE SPATIAL INDEX ON sidewalks_walkways_UNION(THE_GEOM);")

System.out.println('Waterways')
sql.execute("drop table DIFFERENCE_1 if exists;")
sql.execute("create table DIFFERENCE_1 as select ST_DIFFERENCE(b.the_geom, a.the_geom) the_geom from sidewalks_walkways_UNION b, WATERWAYS_UNION a;")

System.out.println('Buildins')
sql.execute("CREATE SPATIAL INDEX ON DIFFERENCE_1(THE_GEOM);"+
"CREATE SPATIAL INDEX ON BUILDINGS_UNION(THE_GEOM);")

System.out.println('Buildings')
sql.execute("drop table DIFFERENCE_2 if exists;")
sql.execute("create table DIFFERENCE_2 as select ST_DIFFERENCE(b.the_geom, a.the_geom) the_geom from BUILDINGS_UNION a, DIFFERENCE_1 b;")

System.out.println('Explode zones')
sql.execute("drop table if exists ZPB_explode;")
sql.execute("create table ZPB_explode as SELECT * FROM ST_Explode('DIFFERENCE_2');")







    resultString = resultString + "<br> Calculation Done !"

    // print to command window
    System.out.println('Result : ' + resultString)
    System.out.println('End : Osm To Input Data')
    System.out.println('Duration : ' + TimeCategory.minus(new Date(), start))

    // print to WPS Builder
    return resultString


}

