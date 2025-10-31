/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by Université Gustave Eiffel and CNRS
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */
/**
 * @Author Pierre Aumond, Université Gustave Eiffel
 * @Author Nicolas Fortin, Université Gustave Eiffel
 * @Author Gwendall Petit, Cerema
 * @Author Part of this file are inspired of https://github.com/orbisgis/geoclimate/wiki
 */

package org.noise_planet.noisemodelling.scripts.Import_and_Export;

import groovy.json.JsonSlurper;
import groovy.sql.Sql
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Coordinate
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import org.openstreetmap.osmosis.xml.v0_6.XmlReader;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;

import crosby.binary.osmosis.OsmosisReader
import org.noise_planet.noisemodelling.wps.Database_Manager.DatabaseHelper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection

title = 'Import Pedestrian tables from OSM'

description = '&#10145;&#65039; Convert <b>.osm</b>, <b>.osm.gz</b> or <b>.osm.pbf</b> file into NoiseModelling input tables.<br><br>' +
        'The following output tables will be created: <br>' +
        '- <b> BUILDINGS </b>: a table containing the buildings<br>' +
        '&#128161; The user can choose to avoid creating some of these tables by checking the dedicated boxes'

inputs = [
        pathFile : [
                name       : 'Path of the OSM file',
                title      : 'Path of the OSM file',
                description: '&#128194; Path of the OSM file, including its extension (.osm, .osm.gz or .osm.pbf).<br>' +
                        'For example: c:/home/area.osm.pbf',
                type       : String.class
        ],
        targetSRID : [
                name       : 'Target projection identifier',
                title      : 'Target projection identifier',
                description: '&#127757; Target projection identifier (also called SRID) of your table.<br>' +
                        'It should be an <a href="https://epsg.io/" target="_blank">EPSG</a> code, an integer with 4 or 5 digits (ex: <a href="https://epsg.io/3857" target="_blank">3857</a> is Web Mercator projection).<br><br>' +
                        '&#10071; The target SRID must be in <b>metric</b> coordinates.',
                type       : Integer.class
        ]
]

outputs = [
        result : [
                name: 'Result output string',
                title: 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type: String.class
        ]
]


// main function of the script
def exec(Connection connection, input) {

    connection = new ConnectionWrapper(connection)

    Sql sql = new Sql(connection)

    String resultString

    Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")
    logger.info('Start : Get Buildings from OSM')
    logger.info("inputs {}", input)

    // -------------------
    // Get every inputs
    // -------------------

    String pathFile = input["pathFile"] as String

    Integer srid = 3857
    if (input['targetSRID']) {
        srid = input['targetSRID'] as Integer
    }

    // Read the OSM file, depending on its extension
    def reader
    if (pathFile.endsWith(".pbf")) {
        InputStream inputStream = new FileInputStream(pathFile);
        reader = new OsmosisReader(inputStream);
    } else if (pathFile.endsWith(".osm")) {
        reader = new XmlReader(new File(pathFile), true, CompressionMethod.None);
    } else if (pathFile.endsWith(".osm.gz")) {
        reader = new XmlReader(new File(pathFile), true, CompressionMethod.GZip);
    }

    OsmHandlerPedestrian handler = new OsmHandlerPedestrian(logger)
    reader.setSink(handler);
    reader.run();

    logger.info('OSM Read done')


    String tableName = "MAP_BUILDINGS_GEOM";

    sql.execute("DROP TABLE IF EXISTS " + tableName)
    sql.execute("CREATE TABLE " + tableName + '''( 
        ID_WAY integer PRIMARY KEY, 
        THE_GEOM geometry,
        HEIGHT real
    );''')

    for (Building_Pedestrian building: handler.buildings) {
        sql.execute("INSERT INTO " + tableName + " VALUES (" + building.id + ", ST_MakeValid(ST_SIMPLIFYPRESERVETOPOLOGY(ST_Transform(ST_GeomFromText('" + building.geom + "', 4326), "+srid+"),0.1)), " + building.height + ")")
    }

    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, tableName, 'the_geom', 'BUILDINGS_INDEX')

    // Identify overlapping buildings (use explicit JOIN for PostgreSQL compatibility)
    sql.execute("DROP TABLE IF EXISTS tmp_relation_buildings_buildings")
    String relationSql = "CREATE TABLE tmp_relation_buildings_buildings AS " +
      "SELECT s1.ID_WAY AS PK_BUILDING, s2.ID_WAY AS PK2_BUILDING " +
      "FROM MAP_BUILDINGS_GEOM s1 " +
      "JOIN MAP_BUILDINGS_GEOM s2 ON s1.ID_WAY <> s2.ID_WAY " +
      "WHERE ST_AREA(s1.THE_GEOM) < ST_AREA(s2.THE_GEOM) " +
      "AND s1.THE_GEOM && s2.THE_GEOM " +
      "AND ST_DISTANCE(s1.THE_GEOM, s2.THE_GEOM) <= 0.1"
    sql.execute(relationSql)

    // Aggregate neighbouring geometries per building before subtracting (avoids GROUP BY geometry issues on PostgreSQL)
    sql.execute("DROP TABLE IF EXISTS tmp_buildings_neighbors")
    sql.execute("CREATE TABLE tmp_buildings_neighbors AS " +
      "SELECT r.PK_BUILDING, ST_Collect(s2.the_geom) AS neighbors_geom " +
      "FROM tmp_relation_buildings_buildings r " +
      "JOIN MAP_BUILDINGS_GEOM s2 ON s2.ID_WAY = r.PK2_BUILDING " +
      "GROUP BY r.PK_BUILDING")

    sql.execute("DROP TABLE IF EXISTS tmp_buildings_truncated")
    sql.execute("CREATE TABLE tmp_buildings_truncated AS " +
      "SELECT s1.ID_WAY AS PK_BUILDING, " +
      "       ST_Difference(s1.the_geom, ST_Buffer(n.neighbors_geom, 0.1, 'join=mitre')) AS the_geom, " +
      "       s1.HEIGHT AS HEIGHT " +
      "FROM MAP_BUILDINGS_GEOM s1 " +
      "JOIN tmp_buildings_neighbors n ON n.PK_BUILDING = s1.ID_WAY " +
      "WHERE n.neighbors_geom IS NOT NULL")

    // Merge original buildings with altered geometries
    sql.execute("DROP TABLE IF EXISTS BUILDINGS")
    String createBuildingsSelect = "SELECT s.id_way AS PK, ST_SetSRID(s.the_geom, " + srid + ") AS THE_GEOM, s.HEIGHT AS HEIGHT " +
      "FROM MAP_BUILDINGS_GEOM s " +
      "WHERE s.id_way NOT IN (SELECT PK_BUILDING FROM tmp_buildings_truncated) " +
      "UNION ALL " +
      "SELECT PK_BUILDING AS PK, ST_SetSRID(the_geom, " + srid + ") AS THE_GEOM, HEIGHT AS HEIGHT " +
      "FROM tmp_buildings_truncated " +
      "WHERE the_geom IS NOT NULL AND NOT ST_IsEmpty(the_geom)"

    if (DatabaseHelper.isPostgreSQL(connection)) {
        sql.execute("CREATE TABLE BUILDINGS AS " + createBuildingsSelect)
        sql.execute("ALTER TABLE BUILDINGS ADD PRIMARY KEY (PK)")
        DatabaseHelper.ensureSRID(connection, "BUILDINGS", "THE_GEOM", srid)
    } else {
        sql.execute("CREATE TABLE BUILDINGS(PK INTEGER PRIMARY KEY, THE_GEOM GEOMETRY, HEIGHT real) AS " + createBuildingsSelect)
    }

    // Clean temporary tables
    sql.execute("DROP TABLE IF EXISTS tmp_buildings_truncated")
    sql.execute("DROP TABLE IF EXISTS tmp_buildings_neighbors")
    sql.execute("DROP TABLE IF EXISTS tmp_relation_buildings_buildings")
    sql.execute("DROP TABLE IF EXISTS MAP_BUILDINGS_GEOM")

    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, "BUILDINGS", 'THE_GEOM', 'BUILDING_GEOM_INDEX')


    sql.execute("DROP TABLE IF EXISTS PEDESTRIAN_WAYS")
    sql.execute("CREATE TABLE PEDESTRIAN_WAYS (PK serial PRIMARY KEY, ID_WAY integer, THE_GEOM geometry, TYPE varchar);")

  String geomTransformExpr = "ST_TRANSFORM(ST_GeomFromText(?, 4326), " + srid + ")"
  String simplifiedGeomExpr
  if (DatabaseHelper.isPostgreSQL(connection)) {
    simplifiedGeomExpr = "ST_SnapToGrid(ST_SimplifyPreserveTopology(" + geomTransformExpr + ", 0.01), 0.1)"
  } else {
    simplifiedGeomExpr = "ST_precisionreducer(ST_SimplifyPreserveTopology(" + geomTransformExpr + ",0.01),0.1)"
  }
  String finalGeomExpr = "st_setsrid(" + simplifiedGeomExpr + ", " + srid + ")"

  for (PedestrianWay pedestrianWay: handler.pedestrianWays) {
        if (pedestrianWay.geom.isEmpty()) {
            continue;
        }
        String query = 'INSERT INTO PEDESTRIAN_WAYS(ID_WAY, ' +
                'THE_GEOM, ' +
                'TYPE ) ' +
                ' VALUES (?,' +
        finalGeomExpr + ',' +
                '?);'
  sql.execute(query, [pedestrianWay.id, pedestrianWay.geom.toText(), pedestrianWay.type])
    }
    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, "PEDESTRIAN_WAYS", 'THE_GEOM', 'PEDESTRIAN_WAYS_GEOM_INDEX')


    sql.execute("DROP TABLE IF EXISTS PEDESTRIAN_POIS")
    sql.execute("CREATE TABLE PEDESTRIAN_POIS (PK serial PRIMARY KEY, ID_WAY BIGINT, THE_GEOM geometry, TYPE varchar);")

    for (PedestrianPOI pedestrianPOI: handler.pedestrianPOIs) {
        if (pedestrianPOI.geom.isEmpty()) {
            continue;
        }
        String query = 'INSERT INTO PEDESTRIAN_POIS(ID_WAY, ' +
                'THE_GEOM, ' +
                'TYPE ) ' +
                ' VALUES (?,' +
                finalGeomExpr + ',' +
                '?);'
  sql.execute(query, [pedestrianPOI.id, pedestrianPOI.geom.toText(), pedestrianPOI.type])
    }
    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, "PEDESTRIAN_POIS", 'THE_GEOM', 'PEDESTRIAN_POIS_GEOM_INDEX')


  sql.execute("DROP TABLE IF EXISTS GROUND")
  String groundTableType = DatabaseHelper.isPostgreSQL(connection) ? "double precision" : "double"
  sql.execute("CREATE TABLE GROUND (PK serial PRIMARY KEY,ID_WAY BIGINT, TYPE varchar, THE_GEOM geometry,PRIORITY int, G " + groundTableType + ");")


    for (Grounds grounds : handler.grounds) {
        if (grounds.priority == 0) {
            continue
        }
        if (grounds.geom.isEmpty()) {
            continue
        }

        String query = 'INSERT INTO GROUND(ID_WAY, THE_GEOM, TYPE, PRIORITY ,G ) ' +
                ' VALUES (?,' +
    finalGeomExpr + ',' +
                '?, ?, ?);'

  sql.execute(query, [grounds.id, grounds.geom.toText(), grounds.type, grounds.priority , grounds.coeff_G])
    }
    // Create spatial index (cross-database compatible)
    DatabaseHelper.createSpatialIndex(connection, "GROUND", 'THE_GEOM', 'GROUND_GEOM_INDEX')

    logger.info('SQL INSERT done')


    String roadWidthCase = """CASE
      WHEN TYPE = 'primary' THEN 6
      WHEN TYPE = 'primary_link' THEN 6
      WHEN TYPE = 'secondary' THEN 6
      WHEN TYPE = 'secondary_link' THEN 6
      WHEN TYPE = 'tertiary' THEN 3.5
      WHEN TYPE = 'tertiary_link' THEN 3.5
      WHEN TYPE = 'motorway' THEN 6
      WHEN TYPE = 'motorway_link' THEN 6
      WHEN TYPE = 'trunk' THEN 6
      WHEN TYPE = 'trunk_link' THEN 6
      WHEN TYPE = 'cycleway' THEN 1.75
      WHEN TYPE = 'residential' THEN 3.5
      WHEN TYPE = 'bus_guideway' THEN 3.5
      WHEN TYPE = 'busway' THEN 3.5
      WHEN TYPE = 'road' THEN 3.5
      WHEN TYPE = 'escape' THEN 3.5
      WHEN TYPE = 'raceway' THEN 3.5
      WHEN TYPE = 'unclassified' THEN 6
      ELSE 0
  END"""

    String sidewalkWidthCase = """CASE
      WHEN TYPE = 'primary' THEN 0
      WHEN TYPE = 'primary_link' THEN 0
      WHEN TYPE = 'secondary' THEN 0
      WHEN TYPE = 'secondary_link' THEN 0
      WHEN TYPE = 'tertiary' THEN 2
      WHEN TYPE = 'tertiary_link' THEN 2
      WHEN TYPE = 'motorway' THEN 0
      WHEN TYPE = 'motorway_link' THEN 0
      WHEN TYPE = 'trunk' THEN 0
      WHEN TYPE = 'trunk_link' THEN 0
      WHEN TYPE = 'cycleway' THEN 1
      WHEN TYPE = 'residential' THEN 2
      WHEN TYPE = 'bus_guideway' THEN 2
      WHEN TYPE = 'busway' THEN 2
      WHEN TYPE = 'road' THEN 1.5
      WHEN TYPE = 'escape' THEN 1.5
      WHEN TYPE = 'raceway' THEN 1.5
      WHEN TYPE = 'unclassified' THEN 1.5
      ELSE 0
  END"""

  def applyPrecision = { String expr ->
    if (DatabaseHelper.isPostgreSQL(connection)) {
      return "ST_SnapToGrid(ST_SimplifyPreserveTopology(" + expr + ", 0.1), 0.1)"
    }
    return "ST_PRECISIONREDUCER(" + expr + ",0.1)"
  }

  def aggregateGeometries = { String expr ->
    if (DatabaseHelper.isPostgreSQL(connection)) {
      return "ST_UNION(" + expr + ")"
    }
    return "ST_UNION(ST_ACCUM(" + expr + "))"
  }

    String roadsSelect = "SELECT ST_FORCE2D(the_geom) AS the_geom, " +
      (DatabaseHelper.isPostgreSQL(connection) ? "(" + roadWidthCase + ")::double precision AS wb" : roadWidthCase + " AS wb") +
      ", PK FROM PEDESTRIAN_WAYS"
    String sidewalkSelect = "SELECT the_geom, " +
      (DatabaseHelper.isPostgreSQL(connection) ? "(" + sidewalkWidthCase + ")::double precision AS lt" : sidewalkWidthCase + " AS lt") +
      ", " + (DatabaseHelper.isPostgreSQL(connection) ? "PK::double precision AS pk" : "PK") +
      " FROM PEDESTRIAN_WAYS"

    String roadsCreate = DatabaseHelper.isPostgreSQL(connection) ?
      "CREATE TABLE ROADS AS " + roadsSelect + ";" :
      "CREATE TABLE ROADS(the_geom geometry, wb float, PK INTEGER) AS " + roadsSelect + ";"

    String sidewalkCreate = DatabaseHelper.isPostgreSQL(connection) ?
      "CREATE TABLE sidewalk AS " + sidewalkSelect + ";" :
      "CREATE TABLE sidewalk(the_geom geometry,lt float, PK float) AS " + sidewalkSelect + ";"

  String roadsSidewalkSelect = "SELECT " + aggregateGeometries(applyPrecision("ST_BUFFER(a.the_geom,a.wb + 2*b.lt)")) + " AS the_geom FROM ROADS a, sidewalk b WHERE a.PK = b.PK"
    String roadsSidewalkCreate = DatabaseHelper.isPostgreSQL(connection) ?
      "CREATE TABLE roads_sidewalk AS " + roadsSidewalkSelect + ";" :
      "CREATE TABLE roads_sidewalk(the_geom geometry) AS " + roadsSidewalkSelect + ";"

  String pedestrianStreetsSelect = "SELECT " + aggregateGeometries(applyPrecision("ST_BUFFER(the_geom,3.0)")) + " AS the_geom FROM PEDESTRIAN_WAYS\n" +
      "            WHERE\n" +
      "            TYPE = 'pedestrian'\n" +
      "            OR TYPE = 'path'\n" +
      "            OR TYPE = 'footway'\n" +
      "            OR TYPE = 'living_street'\n" +
      "            OR TYPE = 'crossing'\n" +
      "            OR TYPE = 'service'\n" +
      "            OR TYPE = 'sidewalk'\n" +
      "            OR TYPE = 'steps'"

  String roadsAreaCreate = "CREATE TABLE ROADS_AREA AS SELECT ST_FORCE2D(" + aggregateGeometries(applyPrecision("ST_BUFFER(the_geom,wb)")) + ") AS the_geom FROM ROADS;"
  String buildingsAreaCreate = "CREATE TABLE BUILDINGS_AREA AS SELECT ST_FORCE2D(" + aggregateGeometries(applyPrecision("ST_BUFFER(the_geom,0.5)")) + ") AS the_geom FROM BUILDINGS;"

    StringBuilder query2Builder = new StringBuilder()
    query2Builder.append("-- Define Road width\n")
    query2Builder.append("DROP TABLE IF EXISTS ROADS;\n")
    query2Builder.append(roadsCreate + "\n\n")
    query2Builder.append("-- Create Sidewalk layer\n")
    query2Builder.append("DROP TABLE IF EXISTS sidewalk;\n")
    query2Builder.append(sidewalkCreate + "\n\n")
    query2Builder.append("-- Create Road + Sidewalk layer\n")
    query2Builder.append("DROP TABLE IF EXISTS roads_sidewalk;\n")
    query2Builder.append(roadsSidewalkCreate + "\n")
    query2Builder.append("DROP TABLE IF EXISTS sidewalk;\n\n")
    query2Builder.append("-- Create PedestrianNetwork\n")
    query2Builder.append("DROP TABLE IF EXISTS pedestrian_streets;\n")
    query2Builder.append("CREATE TABLE pedestrian_streets AS " + pedestrianStreetsSelect + ";\n\n")
    query2Builder.append("-- Create Full area where pedestrians can be\n")
    query2Builder.append("DROP TABLE IF EXISTS RoadsAndPedestrianStreets;\n")
    query2Builder.append("CREATE TABLE RoadsAndPedestrianStreets AS\n")
    query2Builder.append("SELECT * FROM  PEDESTRIAN_STREETS ps UNION SELECT  * FROM  roads_sidewalk rb ;\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS_AREA;\n")
  query2Builder.append("CREATE TABLE PEDESTRIAN_STREETS_AREA AS SELECT " + aggregateGeometries("the_geom") + " AS the_geom FROM RoadsAndPedestrianStreets ps ;\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS;\n")
    query2Builder.append("DROP TABLE IF EXISTS RoadsAndPedestrianStreets;\n")
    query2Builder.append("DROP TABLE IF EXISTS roads_sidewalk;\n\n")
    query2Builder.append("-- Create Areas where Pedestrian can also be\n")
    query2Builder.append("DROP TABLE IF EXISTS Ok_areas;\n")
  query2Builder.append("CREATE TABLE Ok_areas AS SELECT ST_FORCE2D(" + aggregateGeometries("the_geom") + ") AS the_geom FROM GROUND pa WHERE TYPE = 'park';\n\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS_AREA_GO_ZONES;\n")
    query2Builder.append("CREATE TABLE PEDESTRIAN_STREETS_AREA_GO_ZONES AS\n")
    query2Builder.append("SELECT * FROM  PEDESTRIAN_STREETS_AREA ps UNION SELECT * FROM  Ok_areas rb ;\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS_GO_ZONES_AREA;\n")
  query2Builder.append("CREATE TABLE PEDESTRIAN_STREETS_GO_ZONES_AREA AS SELECT " + aggregateGeometries("the_geom") + " AS the_geom FROM PEDESTRIAN_STREETS_AREA_GO_ZONES ps ;\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS_AREA;\n")
    query2Builder.append("DROP TABLE IF EXISTS Ok_areas;\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS_AREA_GO_ZONES;\n\n")
    query2Builder.append("-- Remove Road Areas to final area\n")
    query2Builder.append("-- Create Roads area where Pedestrian can't be\n")
    query2Builder.append("DROP TABLE IF EXISTS ROADS_AREA;\n")
    query2Builder.append(roadsAreaCreate + "\n\n")
    query2Builder.append("DROP TABLE IF EXISTS PSA_ROADS;\n")
    query2Builder.append("CREATE TABLE PSA_ROADS AS\n")
    query2Builder.append("SELECT ST_DIFFERENCE(b.the_geom, a.the_geom) AS the_geom FROM\n")
    query2Builder.append(" ROADS_AREA a,\n PEDESTRIAN_STREETS_GO_ZONES_AREA b;\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_STREETS_GO_ZONES_AREA;\n")
    query2Builder.append("DROP TABLE IF EXISTS ROADS;\n")
    query2Builder.append("DROP TABLE IF EXISTS ROADS_AREA;\n\n")
    query2Builder.append("-- Remove BUILDINGS Areas to final area\n")
    query2Builder.append("DROP TABLE IF EXISTS BUILDINGS_AREA;\n")
    query2Builder.append(buildingsAreaCreate + "\n")
    query2Builder.append("DROP TABLE IF EXISTS PSA_ROADS_BUILDINGS;\n")
    query2Builder.append("CREATE TABLE PSA_ROADS_BUILDINGS AS\n")
    query2Builder.append("SELECT ST_DIFFERENCE(b.the_geom, a.the_geom) AS the_geom FROM\n")
    query2Builder.append(" BUILDINGS_AREA a,\n PSA_ROADS b;\n")
    query2Builder.append("DROP TABLE IF EXISTS PSA_ROADS;\n")
    query2Builder.append("DROP TABLE IF EXISTS BUILDINGS_AREA;\n\n")
    query2Builder.append("-- Remove NOGOZONES Areas to final area\n")
    query2Builder.append("-- Create Areas area where Pedestrian can't be\n")
    query2Builder.append("DROP TABLE IF EXISTS No_GoZones;\n")
  query2Builder.append("CREATE TABLE No_GoZones AS SELECT ST_FORCE2D(" + aggregateGeometries("the_geom") + ") AS the_geom FROM GROUND pa WHERE TYPE = 'water' OR TYPE ='parking';\n\n")
    query2Builder.append("DROP TABLE IF EXISTS PEDESTRIAN_AREA;\n")
    query2Builder.append("CREATE TABLE PEDESTRIAN_AREA AS\n")
    query2Builder.append("SELECT ST_DIFFERENCE(b.the_geom, a.the_geom) AS the_geom FROM\n")
    query2Builder.append(" No_GoZones a,\n PSA_ROADS_BUILDINGS b;\n")
    query2Builder.append("DROP TABLE IF EXISTS PSA_ROADS_BUILDINGS;\n")
    query2Builder.append("DROP TABLE IF EXISTS No_GoZones;")

    String query2 = query2Builder.toString()

    logger.info('SQL Compute Pedestrian Areas')

    sql.execute(query2)


    logger.info('SQL Compute Pedestrian Areas Done !')

    resultString = "nodes : " + handler.nb_nodes
    resultString += "<br>\n"
    resultString += "ways : " + handler.nb_ways
    resultString += "<br>\n"
    resultString += "relations : " + handler.nb_relations
    resultString += "<br>\n"
    resultString += "buildings : " + handler.nb_buildings
    resultString += "<br>\n"
    resultString += "pedestrianWays : " + handler.nb_pedestrianWays
    resultString += "<br>\n"
    resultString += "pedestrianAreas : " + handler.nb_pedestrianAreas
    resultString += "<br>\n"

    logger.info('End : Get Buildings from OSM')
    logger.info('Result : ' + resultString)
    return resultString
}

public class OsmHandlerPedestrian implements Sink {

    public int nb_ways = 0;
    public int nb_nodes = 0;
    public int nb_relations = 0;
    public int nb_noGoZones = 0;
    public int nb_buildings = 0;
    public int nb_pedestrianWays = 0;
    public int nb_pedestrianAreas = 0;
    public int nb_pedestrianPOI = 0;

    Random rand = new Random();

    public Map<Long, Node> nodes = new HashMap<Long, Node>();
    public Map<Long, Way> ways = new HashMap<Long, Way>();
    public Map<Long, Relation> relations = new HashMap<Long, Relation>();
    public List<Building_Pedestrian> buildings = new ArrayList<Building_Pedestrian>();
    public List<PedestrianWay> pedestrianWays = new ArrayList<PedestrianWay>();
    public List<Grounds> grounds = new ArrayList<Grounds>();
    public List<PedestrianPOI> pedestrianPOIs = new ArrayList<PedestrianPOI>();

    Logger logger


    OsmHandlerPedestrian(Logger logger) {
        this.logger = logger

    }

    @Override
    public void initialize(Map<String, Object> arg0) {
    }

    @Override
    public void process(EntityContainer entityContainer) {
        if (entityContainer instanceof NodeContainer) {
            nb_nodes++;
            Node node = ((NodeContainer) entityContainer).getEntity();
            nodes.put(node.getId(), node);
            boolean isPedestrianPOI = false;
            for (Tag tag : node.getTags()) {
                if ("tourism".equalsIgnoreCase(tag.getKey())) {
                    List list = ["hotel", "guest-house", "apartment", "hostel"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "tourism_sleep"));
                        nb_pedestrianPOI++;
                    }
                    list = ["information","attraction", "artwork", "viewpoint", "museum", "gallery", "yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "tourism"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("shop".equalsIgnoreCase(tag.getKey())) {
                    isPedestrianPOI = true;
                    pedestrianPOIs.add(new PedestrianPOI(node, "shop"));
                    nb_pedestrianPOI++;
                }

                if ("place".equalsIgnoreCase(tag.getKey())) {
                    List list = ["square"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "place"));
                        nb_pedestrianPOI++;
                    }
                }
                if ("natural".equalsIgnoreCase(tag.getKey())) {
                    List list = ["tree","tree_row"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "trees"));
                        nb_pedestrianPOI++;
                    }
                    list = ["water"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "water"));
                        nb_pedestrianPOI++;
                    }
                    list = ["grass"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "grass"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("crossing".equalsIgnoreCase(tag.getKey())) {
                    List list = ["marked","unmarked","uncontrolled","traffic_signals","zebra"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "footpath"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("public_transport".equalsIgnoreCase(tag.getKey())) {
                    List list = ["platform","stop_position","stop_area","station","pole"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "public_transport"));
                        nb_pedestrianPOI++;
                    }
                }
                if ("bus".equalsIgnoreCase(tag.getKey())) {
                    List list = ["yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "public_transport"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("railway".equalsIgnoreCase(tag.getKey())) {
                    List list = ["platform","tram_stop","subway_entrance"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "public_transport"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("leisure".equalsIgnoreCase(tag.getKey())) {
                    List list = ["pitch","swimming_pool","sports_centre","fitness_centre","fitness_station","swimming_area"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "sport"));
                        nb_pedestrianPOI++;
                    }
                    list = ["park","garden"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "grass"));
                        nb_pedestrianPOI++;
                    }

                    list = ["playground","picnic_table","outdoor_seating","common"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "leisure"));
                        nb_pedestrianPOI++;
                    }

                }

                if ("bench".equalsIgnoreCase(tag.getKey())) {
                    List list = ["yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "leisure"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("foot".equalsIgnoreCase(tag.getKey())) {
                    List list = ["yes","designated","use_sidepath"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "footpath"));
                        nb_pedestrianPOI++;
                    }
                }
                if ("religion".equalsIgnoreCase(tag.getKey())) {
                    isPedestrianPOI = true;
                    pedestrianPOIs.add(new PedestrianPOI(node, "religion"));
                    nb_pedestrianPOI++;
                }


                if ("bicycle_parking".equalsIgnoreCase(tag.getKey())) {
                    List list = ["stands","wall_llops","rack","shed","bollard"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "individual_transport"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("parking".equalsIgnoreCase(tag.getKey())) {
                    List list = ["surface","street_side","underground","multi-storey","lane","yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "individual_transport"));
                        nb_pedestrianPOI++;
                    }
                }

                if ("sport".equalsIgnoreCase(tag.getKey())) {
                    isPedestrianPOI = true;
                    pedestrianPOIs.add(new PedestrianPOI(node, "sport"));
                    nb_pedestrianPOI++;
                }
                if ("amenity".equalsIgnoreCase(tag.getKey())) {
                    List list = ["parking","bicycle_parking"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "individual_transport"));
                        nb_pedestrianPOI++;
                    }

                    list = ["bus_station"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "public_transport"));
                        nb_pedestrianPOI++;
                    }

                    list = ["bank", "pharmarcy","marketplace"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "shop"));
                        nb_pedestrianPOI++;
                    }

                    list = ["bench"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "leisure"));
                        nb_pedestrianPOI++;
                    }

                    list = ["cafe", "fast_food","bar","pub","nightclub","food_court","biergarten","casino"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "food_drink"));
                        nb_pedestrianPOI++;
                    }

                    list = ["place_of_worship"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "religion"));
                        nb_pedestrianPOI++;
                    }

                    list = ["school","college","university"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "education"));
                        nb_pedestrianPOI++;
                    }

                    list = ["theatre","cinema"]
                    if (list.any { it == (tag.getValue()) }) {
                        isPedestrianPOI = true;
                        pedestrianPOIs.add(new PedestrianPOI(node, "culture"));
                        nb_pedestrianPOI++;
                    }

                }




            }

        } else if (entityContainer instanceof WayContainer) {

            // This is a copy of the GeoClimate file : buildingsParams.json (https://github.com/orbisgis/geoclimate/tree/master/osm/src/main/resources/org/orbisgis/geoclimate/osm)
            String buildingParams = """{
                  "tags": {
                    "building": [],
                    "railway": [
                      "station",
                      "train_station"
                    ]
                  },
                  "columns": [
                    "height",
                    "roof:height",
                    "building:levels",
                    "roof:levels",
                    "building",
                    "amenity",
                    "layer",
                    "aeroway",
                    "historic",
                    "leisure",
                    "monument",
                    "place_of_worship",
                    "military",
                    "railway",
                    "public_transport",
                    "barrier",
                    "government",
                    "historic:building",
                    "grandstand",
                    "house",
                    "shop",
                    "industrial",
                    "man_made",
                    "residential",
                    "apartments",
                    "ruins",
                    "agricultural",
                    "barn",
                    "healthcare",
                    "education",
                    "restaurant",
                    "sustenance",
                    "office",
                    "tourism",
                    "roof:shape"
                  ],
                  "level": {
                    "building": 1,
                    "house": 1,
                    "detached": 1,
                    "residential": 1,
                    "apartments": 1,
                    "bungalow": 0,
                    "historic": 0,
                    "monument": 0,
                    "ruins": 0,
                    "castle": 0,
                    "agricultural": 0,
                    "farm": 0,
                    "farm_auxiliary": 0,
                    "barn": 0,
                    "greenhouse": 0,
                    "silo": 0,
                    "commercial": 2,
                    "industrial": 0,
                    "sport": 0,
                    "sports_centre": 0,
                    "grandstand": 0,
                    "transportation": 0,
                    "train_station": 0,
                    "toll_booth": 0,
                    "toll": 0,
                    "terminal": 0,
                    "healthcare": 1,
                    "education": 1,
                    "entertainment_arts_culture": 0,
                    "sustenance": 1,
                    "military": 0,
                    "religious": 0,
                    "chapel": 0,
                    "church": 0,
                    "government": 1,
                    "townhall": 1,
                    "office": 1,
                    "heavy_industry": 0,
                    "light_industry": 0,
                    "emergency": 0,
                    "hotel": 2,
                    "hospital": 2,
                    "parking": 1
                  },
                  "type": {
                    "terminal:transportation": {
                      "aeroway": [
                        "terminal",
                        "airport_terminal"
                      ],
                      "amenity": [
                        "terminal",
                        "airport_terminal"
                      ],
                      "building": [
                        "terminal",
                        "airport_terminal"
                      ]
                    },
                    "parking:transportation": {
                      "building": [
                        "parking"
                      ]
                    },
                    "monument": {
                      "building": [
                        "monument"
                      ],
                      "historic": [
                        "monument"
                      ],
                      "leisure": [
                        "monument"
                      ],
                      "monument": [
                        "yes"
                      ]
                    },
                    "chapel:religious": {
                      "building": [
                        "chapel"
                      ],
                      "amenity": [
                        "chapel"
                      ],
                      "place_of_worship": [
                        "chapel"
                      ]
                    },
                    "church:religious": {
                      "building": [
                        "church"
                      ],
                      "amenity": [
                        "church"
                      ],
                      "place_of_worship": [
                        "church"
                      ]
                    },
                    "castle:heritage": {
                      "building": [
                        "castle",
                        "fortress"
                      ]
                    },
                    "religious": {
                      "building": [
                        "religious",
                        "abbey",
                        "cathedral",
                        "mosque",
                        "musalla",
                        "temple",
                        "synagogue",
                        "shrine",
                        "place_of_worship",
                        "wayside_shrine"
                      ],
                      "amenity": [
                        "religious",
                        "abbey",
                        "cathedral",
                        "chapel",
                        "church",
                        "mosque",
                        "musalla",
                        "temple",
                        "synagogue",
                        "shrine",
                        "place_of_worship",
                        "wayside_shrine"
                      ],
                      "place_of_worship": [
                        "! no",
                        "! chapel",
                        "! church"
                      ]
                    },
                    "sport:entertainment_arts_culture": {
                      "building": [
                        "swimming_pool",
                        "fitness_centre",
                        "horse_riding",
                        "ice_rink",
                        "pitch",
                        "stadium",
                        "track"
                      ],
                      "leisure": [
                        "swimming_pool",
                        "fitness_centre",
                        "horse_riding",
                        "ice_rink",
                        "pitch",
                        "stadium",
                        "track"
                      ],
                      "amenity": [
                        "swimming_pool",
                        "fitness_centre",
                        "horse_riding",
                        "ice_rink",
                        "pitch",
                        "stadium",
                        "track"
                      ]
                    },
                    "sports_centre:entertainment_arts_culture": {
                      "building": [
                        "sports_centre",
                        "sports_hall"
                      ],
                      "leisure": [
                        "sports_centre",
                        "sports_hall"
                      ],
                      "amenity": [
                        "sports_centre",
                        "sports_hall"
                      ]
                    },
                    "military": {
                      "military": [
                        "ammunition",
                        "bunker",
                        "barracks",
                        "casemate",
                        "office",
                        "shelter"
                      ],
                      "building": [
                        "ammunition",
                        "bunker",
                        "barracks",
                        "casemate",
                        "military",
                        "shelter"
                      ],
                      "office": [
                        "military"
                      ]
                    },
                    "train_station:transportation": {
                      "building": [
                        "train_station"
                      ],
                      "railway": [
                        "station",
                        "train_station"
                      ],
                      "public_transport": [
                        "train_station"
                      ],
                      "amenity": [
                        "train_station"
                      ]
                    },
                    "townhall:government": {
                      "amenity": [
                        "townhall"
                      ],
                      "building": [
                        "townhall"
                      ]
                    },
                    "toll:transportation": {
                      "barrier": [
                        "toll_booth"
                      ],
                      "building": [
                        "toll_booth"
                      ]
                    },
                    "government": {
                      "building": [
                        "government",
                        "government_office"
                      ],
                      "government": [
                        "! no"
                      ],
                      "office": [
                        "government"
                      ]
                    },
                    "historic": {
                      "building": [
                        "historic"
                      ],
                      "historic": [
                      ],
                      "historic_building": [
                        "! no"
                      ]
                    },
                    "grandstand:entertainment_arts_culture": {
                      "building": [
                        "grandstand"
                      ],
                      "leisure": [
                        "grandstand"
                      ],
                      "amenity": [
                        "grandstand"
                      ],
                      "grandstand": [
                        "yes"
                      ]
                    },
                    "detached:residential": {
                      "building": [
                        "detached"
                      ],
                      "house": [
                        "detached"
                      ]
                    },
                    "farm_auxiliary:agricultural": {
                      "building": [
                        "farm_auxiliary",
                        "barn",
                        "stable",
                        "sty",
                        "cowshed",
                        "digester",
                        "greenhouse"
                      ]
                    },
                    "commercial": {
                      "building": [
                        "bank",
                        "bureau_de_change",
                        "boat_rental",
                        "car_rental",
                        "commercial",
                        "internet_cafe",
                        "kiosk",
                        "money_transfer",
                        "market",
                        "market_place",
                        "pharmacy",
                        "post_office",
                        "retail",
                        "shop",
                        "store",
                        "supermarket",
                        "warehouse"
                      ],
                      "amenity": [
                        "bank",
                        "bureau_de_change",
                        "boat_rental",
                        "car_rental",
                        "commercial",
                        "internet_cafe",
                        "kiosk",
                        "money_transfer",
                        "market",
                        "market_place",
                        "pharmacy",
                        "post_office",
                        "retail",
                        "shop",
                        "store",
                        "supermarket",
                        "warehouse"
                      ],
                      "shop": [
                        "!= no"
                      ]
                    },
                    "light_industry:industrial": {
                      "building": [
                        "industrial",
                        "factory",
                        "warehouse"
                      ],
                      "industrial": [
                        "factory"
                      ],
                      "amenity": [
                        "factory"
                      ]
                    },
                    "heavy_industry:industrial": {
                      "building": [
                        "digester"
                      ],
                      "industrial": [
                        "gas",
                        "heating_station",
                        "oil_mill",
                        "oil",
                        "wellsite",
                        "well_cluster"
                      ]
                    },
                    "greenhouse:agricultural": {
                      "building": [
                        "greenhouse"
                      ],
                      "amenity": [
                        "greenhouse"
                      ],
                      "industrial": [
                        "greenhouse"
                      ]
                    },
                    "silo:agricultural": {
                      "building": [
                        "silo",
                        "grain_silo"
                      ],
                      "man_made": [
                        "silo",
                        "grain_silo"
                      ]
                    },
                    "house:residential": {
                      "building": [
                        "house"
                      ],
                      "house": [
                        "! no",
                        "! detached",
                        "! residential",
                        "! villa",
                        "residential"
                      ],
                      "amenity": [
                        "house"
                      ]
                    },
                    "apartments:residential": {
                      "building": [
                        "apartments"
                      ],
                      "residential": [
                        "apartments"
                      ],
                      "amenity": [
                        "apartments"
                      ],
                      "apartments": [
                        "yes"
                      ]
                    },
                    "bungalow:residential": {
                      "building": [
                        "bungalow"
                      ],
                      "house": [
                        "bungalow"
                      ],
                      "amenity": [
                        "bungalow"
                      ]
                    },
                    "residential": {
                      "building": [
                        "residential",
                        "villa",
                        "dormitory",
                        "condominium",
                        "sheltered_housing",
                        "workers_dormitory",
                        "terrace"
                      ],
                      "residential": [
                        "university",
                        "detached",
                        "dormitory",
                        "condominium",
                        "sheltered_housing",
                        "workers_dormitory",
                        "building"
                      ],
                      "house": [
                        "residential"
                      ],
                      "amenity": [
                        "residential"
                      ]
                    },
                    "ruins:heritage": {
                      "building": [
                        "ruins"
                      ],
                      "ruins": [
                        "ruins"
                      ]
                    },
                    "agricultural": {
                      "building": [
                        "agricultural"
                      ],
                      "agricultural": [
                        "building"
                      ]
                    },
                    "farm:agricultural": {
                      "building": [
                        "farm",
                        "farmhouse"
                      ]
                    },
                    "barn:agricultural": {
                      "building": [
                        "barn"
                      ],
                      "barn": [
                        "! no"
                      ]
                    },
                    "transportation": {
                      "building": [
                        "train_station",
                        "transportation",
                        "station"
                      ],
                      "aeroway": [
                        "hangar",
                        "tower",
                        "bunker",
                        "control_tower",
                        "building"
                      ],
                      "railway": [
                        "station",
                        "train_station",
                        "building"
                      ],
                      "public_transport": [
                        "train_station",
                        "station"
                      ],
                      "amenity": [
                        "train_station",
                        "terminal"
                      ]
                    },
                    "healthcare": {
                      "amenity": [
                        "healthcare",
                        "social_facility"
                      ],
                      "building": [
                        "healthcare",
                        "hospital"
                      ],
                      "healthcare": [
                        "! no"
                      ]
                    },
                    "education": {
                      "amenity": [
                        "education",
                        "college",
                        "kindergarten",
                        "school",
                        "university",
                        "research_institute"
                      ],
                      "building": [
                        "education",
                        "college",
                        "kindergarten",
                        "school",
                        "university"
                      ],
                      "education": [
                        "college",
                        "kindergarten",
                        "school",
                        "university"
                      ]
                    },
                    "entertainment_arts_culture": {
                      "leisure": [
                        "! no"
                      ]
                    },
                    "sustenance:commercial": {
                      "amenity": [
                        "restaurant",
                        "bar",
                        "cafe",
                        "fast_food",
                        "ice_cream",
                        "pub"
                      ],
                      "building": [
                        "restaurant",
                        "bar",
                        "cafe",
                        "fast_food",
                        "ice_cream",
                        "pub"
                      ],
                      "restaurant": [
                        "! no"
                      ],
                      "shop": [
                        "restaurant",
                        "bar",
                        "cafe",
                        "fast_food",
                        "ice_cream",
                        "pub"
                      ],
                      "sustenance": [
                        "! no"
                      ]
                    },
                    "office": {
                      "building": [
                        "office"
                      ],
                      "amenity": [
                        "office"
                      ],
                      "office": [
                        "! no"
                      ]
                    },
                    "building:public": {
                      "building": [
                        "public"
                      ]
                    },
                    "emergency": {
                      "building": [
                        "fire_station"
                      ]
                    },
                    "hotel:tourism": {
                      "building": [
                        "hotel"
                      ],
                      "tourism": [
                        "hotel"
                      ]
                    },
                    "attraction:tourism": {
                      "tourism": [
                        "attraction"
                      ]
                    },
                    "building": {
                      "building": [
                        "yes"
                      ]
                    }
                  }
                }"""

            def parametersMap = new JsonSlurper().parseText(buildingParams)
            def tags = parametersMap.get("tags")
            def columnsToKeep = parametersMap.get("columns")
            def typeBuildings = parametersMap.get("type")

            nb_ways++;
            Way way = ((WayContainer) entityContainer).getEntity();
            ways.put(way.getId(), way);
            boolean isBuilding = false;
            boolean isTunnel = false;
            boolean isPedestrianWay = false;
            double height = 4.0 + rand.nextDouble() * 2.1;
            boolean trueHeightFound = false;
            String type =null;
            boolean closedWay = way.isClosed();

            for (Tag tag : way.getTags()) {
                if (tags.containsKey(tag.getKey()) && closedWay){
                    if (tags.get(tag.getKey()).isEmpty() || tags.get(tag.getKey()).any{it == (tag.getValue())})
                    {
                        isBuilding = true;
                    }
                }

                if ( closedWay && columnsToKeep.any{ (it == tag.getKey()) }) {
                    for (typeHighLevel in typeBuildings) {
                        for (typeLowLevel in typeHighLevel.getValue()) {
                            if (typeLowLevel.getKey() == (tag.getKey())) {
                                if (typeLowLevel.getValue().any { it == (tag.getValue()) }) {
                                    isBuilding = true;
                                }
                            }

                        }
                    }
                }

                if ("tourism".equalsIgnoreCase(tag.getKey())) {
                    List list = ["hotel", "guest-house", "apartment", "hostel"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "tourism_sleep";
                    }
                    list = ["information","attraction", "artwork", "viewpoint", "museum", "gallery", "yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "tourism";
                    }
                    list = ["information", "museum", "gallery", "yes","hotel", "guest-house", "apartment", "hostel"]
                    if (list.any { it == (tag.getValue()) }) {
                        if (way.isClosed()) {isBuilding = true}
                    }

                }

                if ("shop".equalsIgnoreCase(tag.getKey())) {
                    type = "shop";
                    if (way.isClosed()) {isBuilding = true}
                }

                if ("place".equalsIgnoreCase(tag.getKey())) {
                    List list = ["square"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "place";
                    }
                }
                if ("natural".equalsIgnoreCase(tag.getKey())) {
                    List list = ["tree","tree_row"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "vegetation";
                    }
                    list = ["water"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "water";
                    }
                    list = ["grass"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "vegetation";
                    }
                }

                if ("crossing".equalsIgnoreCase(tag.getKey())) {
                    List list = ["marked","unmarked","uncontrolled","traffic_signals","zebra"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "footpath";
                    }
                }

                if ("public_transport".equalsIgnoreCase(tag.getKey())) {
                    List list = ["platform","stop_position","stop_area","station","pole"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "public_transport";
                    }
                }
                if ("bus".equalsIgnoreCase(tag.getKey())) {
                    List list = ["yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "public_transport";
                    }
                }

                if ("railway".equalsIgnoreCase(tag.getKey())) {
                    List list = ["platform","tram_stop","subway_entrance"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "public_transport";
                    }
                }

                if ("leisure".equalsIgnoreCase(tag.getKey())) {
                    List list = ["pitch","swimming_pool","sports_centre","fitness_centre","fitness_station","swimming_area"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "sport";
                    }
                    list = ["swimming_pool","sports_centre","fitness_centre"]
                    if (list.any { it == (tag.getValue()) }) {
                        if (way.isClosed()) {isBuilding = true}
                    }

                    list = ["park","garden"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "vegetation";
                    }

                    list = ["playground","picnic_table","outdoor_seating","common"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "leisure";
                    }

                }

                if ("bench".equalsIgnoreCase(tag.getKey())) {
                    List list = ["yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "leisure";
                    }
                }

                if ("foot".equalsIgnoreCase(tag.getKey())) {
                    List list = ["yes","designated","use_sidepath"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "footpath";
                    }
                }
                if ("religion".equalsIgnoreCase(tag.getKey())) {
                    type = "religion";
                    if (way.isClosed()) {isBuilding = true}
                }


                if ("bicycle_parking".equalsIgnoreCase(tag.getKey())) {
                    List list = ["stands","wall_llops","rack","shed","bollard"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "individual_transport";
                    }
                }

                if ("parking".equalsIgnoreCase(tag.getKey())) {
                    List list = ["surface","street_side","underground","multi-storey","lane","yes"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "individual_transport";
                    }
                }

                if ("sport".equalsIgnoreCase(tag.getKey())) {
                    type = "sport";
                }
                if ("amenity".equalsIgnoreCase(tag.getKey())) {
                    List list = ["parking","bicycle_parking"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "individual_transport";
                    }

                    list = ["bus_station"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "public_transport";
                    }

                    list = ["bank", "pharmarcy","marketplace"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "shop";
                        if (way.isClosed()) {isBuilding = true}
                    }

                    list = ["bench"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "leisure";
                    }

                    list = ["cafe", "fast_food","bar","pub","nightclub","food_court","biergarten","casino"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "food_drink";
                        if (way.isClosed()) {isBuilding = true}
                    }

                    list = ["place_of_worship"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "religion";
                        if (way.isClosed()) {isBuilding = true}
                    }

                    list = ["school","college","university"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "education";
                        if (way.isClosed()) {isBuilding = true}
                    }

                    list = ["theatre","cinema"]
                    if (list.any { it == (tag.getValue()) }) {
                        type = "x";
                        if (way.isClosed()) {isBuilding = true}
                    }

                }

                if ("building".equalsIgnoreCase(tag.getKey())) {
                    isBuilding = true;
                }

                if ("tunnel".equalsIgnoreCase(tag.getKey()) && "yes".equalsIgnoreCase(tag.getValue())) {
                    isTunnel = true;
                }

                if ("highway".equalsIgnoreCase((tag.getKey()))) {
                    isPedestrianWay = true
                }

                if ("height".equalsIgnoreCase(tag.getKey()) && way.isClosed()) {
                    isBuilding = true;
                }

                if (isBuilding) {
                    if (!trueHeightFound && !tag.getValue().replaceAll("[^0-9]+", "").isEmpty() && "building:levels".equalsIgnoreCase(tag.getKey())) {
                        height = height - 4 + Double.parseDouble(tag.getValue().replaceAll("[^0-9]+", "")) * 3.0;
                    }
                    if ("height".equalsIgnoreCase(tag.getKey()) && !tag.getValue().replaceAll("[^0-9]+", "").isEmpty()) {
                        height = Double.parseDouble(tag.getValue().replaceAll("[^0-9]+", ""));
                        trueHeightFound = true;
                    }
                }
                if (!isBuilding && !isPedestrianWay && way.isClosed()) {
                    grounds.add(new Grounds(way,tag.getKey(), tag.getValue(), type));
                    nb_pedestrianAreas++;
                }
            }

            if (isBuilding && way.isClosed()) {
                buildings.add(new Building_Pedestrian(way, height, type));
                nb_buildings++;
            }

            if (isPedestrianWay) {
                if (isTunnel) {
                    return
                }
                pedestrianWays.add(new PedestrianWay(way));
                nb_pedestrianWays++;
            }



        } else if (entityContainer instanceof RelationContainer) {
            nb_relations++;
            Relation rel = ((RelationContainer) entityContainer).getEntity();
            relations.put(rel.getId(), rel);
        } else {
            System.out.println("Unknown Entity!");
        }
    }

    @Override
    public void complete() {
        for(Building_Pedestrian building: buildings) {
            building.setGeom(calculateBuildingGeometry(building.way));
        }
        for(PedestrianWay pedestrianWay: pedestrianWays) {
            pedestrianWay.setGeom(calculatePedestrianWayGeometry(pedestrianWay.way));
        }
        for(PedestrianPOI pedestrianPOI: pedestrianPOIs) {
            pedestrianPOI.setGeom(calculatePedestrianNodeGeometry(pedestrianPOI.node));
        }

        GeometryFactory geomFactory = new GeometryFactory();
        for(Grounds pedestrianArea: grounds) {
            if (pedestrianArea.priority == 0) {
                pedestrianArea.setGeom(geomFactory.createPolygon())
                continue
            }
            Geometry geom = calculatePedestrianAreaGeometry(pedestrianArea.way)
            pedestrianArea.setGeom(geom)
        }
        int doPrint = 2
        for (int j = 0; j < grounds.size(); j++) {
            if (j >= doPrint) {
                logger.info("Cleaning GROUND geom : " + j + "/" + grounds.size())
                doPrint *= 2
            }
            if (grounds[j].geom.isEmpty() || !grounds[j].geom.isValid()) {
                continue
            }
            if (!["Polygon", "MultiPolygon"].contains(grounds[j].geom.geometryType)) {
                continue
            }
            for (int k = 0; k < grounds.size(); k++) {
                if (j == k) {
                    continue
                }
                if (grounds[k].geom.isEmpty() || !grounds[k].geom.isValid()) {
                    continue
                }
                if (!["Polygon", "MultiPolygon"].contains(grounds[k].geom.geometryType)) {
                    continue
                }
                if (!grounds[j].geom.intersects(grounds[k].geom)) {
                    continue
                }
                if (grounds[j].priority >= grounds[k].priority) {
                    grounds[k].geom = grounds[k].geom.difference(grounds[j].geom)
                }
                if (grounds[j].priority < grounds[k].priority) {
                    grounds[j].geom = grounds[j].geom.difference(grounds[k].geom)
                }
            }
        }
    }

    @Override
    public void close() {
    }

    public Geometry calculateBuildingGeometry(Way way) {
        GeometryFactory geomFactory = new GeometryFactory();
        if (way == null) {
            return geomFactory.createPolygon();
        }
        List<WayNode> wayNodes = way.getWayNodes();
        if (wayNodes.size() < 4) {
            return geomFactory.createPolygon();
        }
        Coordinate[] shell = new Coordinate[wayNodes.size()];
        for(int i = 0; i < wayNodes.size(); i++) {
            Node node = nodes.get(wayNodes.get(i).getNodeId());
            if (node == null) {
                return geomFactory.createPolygon();
            }
            double x = node.getLongitude();
            double y = node.getLatitude();
            shell[i] = new Coordinate(x, y, 0.0);
        }
        return geomFactory.createPolygon(shell);
    }

    public Geometry calculatePedestrianNodeGeometry(Node node) {
        GeometryFactory geomFactory = new GeometryFactory();
        if (node == null) {
            return geomFactory.createPoint();
        }
        return geomFactory.createPoint(new Coordinate(node.getLongitude(),node.getLatitude(), 0.0));
    }

    public Geometry calculatePedestrianWayGeometry(Way way) {
        GeometryFactory geomFactory = new GeometryFactory();
        if (way == null) {
            return geomFactory.createLineString();
        }
        List<WayNode> wayNodes = way.getWayNodes();
        if (wayNodes.size() < 2) {
            return geomFactory.createLineString();
        }
        Coordinate[] coordinates = new Coordinate[wayNodes.size()];
        for(int i = 0; i < wayNodes.size(); i++) {
            Node node = nodes.get(wayNodes.get(i).getNodeId());
            double x = node.getLongitude();
            double y = node.getLatitude();
            coordinates[i] = new Coordinate(x, y, 0.0);
        }
        return geomFactory.createLineString(coordinates);
    }

    public Geometry calculatePedestrianAreaGeometry(Way way) {
        GeometryFactory geomFactory = new GeometryFactory();
        if (way == null) {
            return geomFactory.createPolygon();
        }
        List<WayNode> wayNodes = way.getWayNodes();
        if (wayNodes.size() < 4) {
            return geomFactory.createPolygon();
        }
        Coordinate[] shell = new Coordinate[wayNodes.size()];
        for (int i = 0; i < wayNodes.size(); i++) {
            Node node = nodes.get(wayNodes.get(i).getNodeId());
            double x = node.getLongitude();
            double y = node.getLatitude();
            shell[i] = new Coordinate(x, y, 0.0);
        }
        return geomFactory.createPolygon(shell);
    }
}

public class Building_Pedestrian {

    long id;
    Way way;
    Geometry geom;
    double height = 0.0;
    String type;

    Building_Pedestrian(Way way, String type) {
        this.way = way;
        this.id = way.getId();
        this.type = type;
        double h = 4.0 + rand.nextDouble() * 2.1;
        boolean trueHeightFound = false;
        for (Tag tag : way.getTags()) {
            if (!trueHeightFound && "building:levels".equalsIgnoreCase(tag.getKey())) {
                h = h - 4 + Double.parseDouble(tag.getValue().replaceAll("[^0-9]+", "")) * 3.0;
            }
            if ("height".equalsIgnoreCase(tag.getKey())) {
                h = Double.parseDouble(tag.getValue().replaceAll("[^0-9]+", ""));
                trueHeightFound = true;
            }
        }
        this.height = h;
    }

    Building_Pedestrian(Way way, double height, String type) {
        this.way = way;
        this.id = way.getId();
        this.height = height;
        this.type = type;
    }

    void setGeom(Geometry geom) {
        this.geom = geom;
    }

    void setHeight(double height) {
        this.height = height;
    }
}

public class PedestrianWay {

    long id;
    Way way;
    Geometry geom;
    boolean oneway = false;
    String type = null;
    int category = 5;

    PedestrianWay(Way way) {
        this.way = way;
        this.id = way.getId();
        for (Tag tag : way.getTags()) {
            if ("highway".equalsIgnoreCase(tag.getKey())) {
                this.type = tag.getValue();
            }
            if ("highway".equalsIgnoreCase(tag.getKey()) && "yes".equalsIgnoreCase(tag.getValue())) {
                oneway = true
            }
        }
        updateCategory();
    }

    void updateCategory() {
        if (["motorway", "motorway_link"].contains(type)) {
            category = 0
        }
        if (["trunk", "trunk_link"].contains(type)) {
            category = 1
        }
        if (["primary", "primary_link"].contains(type)) {
            category = 2
        }
        if (["secondary", "secondary_link"].contains(type)) {
            category = 3
        }
        if (["tertiary", "tertiary_link", "unclassified"].contains(type)) {
            category = 4
        }
        if (["residential"].contains(type)) {
            category = 5
        }
        if (["service", "living_street"].contains(type)) {
            category = 6
        }
        if (["footway", "path", "crossing", "steps", "pedestrian"].contains(type)) {
            category = 7
        }
    }

    void setGeom(Geometry geom) {
        this.geom = geom;
    }

    void setHeight(double height) {
        this.height = height;
    }
}

public class PedestrianPOI {

    long id;
    Node node;
    Geometry geom;
    String type = null;

    PedestrianPOI(Node node, String type) {
        this.node = node;
        this.id = node.getId();
        this.type = type;


    }

    void setGeom(Geometry geom) {
        this.geom = geom;
    }


}

public class Grounds {

    long id;
    Way way;
    Geometry geom;
    String type ;
    int priority = 0;
    float coeff_G = 0.0;

    Grounds(Way way, String wayTag, String wayKey, String type) {
        this.way = way;
        this.id = way.getId();
        this.type = type;
        String primaryTagKey = "";
        String primaryTagValue = "";
        String secondaryTagKey = "";
        String secondaryTagValue = "";

        // for (Tag tag : way.getTags()) {
        String key = wayKey
        String value = wayTag
        if (["aeroway","amenity","landcover","landuse","leisure","natural","water","waterway"].contains(key)) {
            primaryTagKey = key
            primaryTagValue = value
        }
        if (["parking","covered","surface","wetland"].contains(key)) {
            secondaryTagKey = key
            secondaryTagValue = value
        }
        //}
        if (primaryTagKey == "aeroway" &&
                ["taxiway","runway","aerodrome","helipad","apron","taxilane"].contains(primaryTagValue)) {
            priority = 29
            coeff_G = 0.1
        }
        if (primaryTagKey == "amenity") {
            if (primaryTagValue == "taxi") {
                priority = 22
                coeff_G = 0.1
            }
            if (primaryTagValue == "taxi") {
                priority = 22
                coeff_G = 0.1
            }
            if (primaryTagValue == "parking" && secondaryTagKey == "parking" && !secondaryTagValue.contains("underground")) {
                priority = 22
                coeff_G = 0.1
                type = "parking"
            }
        }
        if (primaryTagKey == "landcover") {
            if (primaryTagValue == "water") {
                priority = 7
                coeff_G = 0.3
                type = "water"
            }
            if (["bedrock","bare_ground","concrete","asphalt"].contains(primaryTagValue)) {
                priority = 8
                coeff_G = 0.1
            }
            if (primaryTagValue == "sand") {
                priority = 9
                coeff_G = 0.2
            }
            if (["scrub","gravel"].contains(primaryTagValue)) {
                priority = 10
                coeff_G = 0.7
            }
            if (["bushes","vegetation"].contains(primaryTagValue)) {
                priority = 11
                coeff_G = 0.8
                type = "vegetation"
            }
            if (["flowerbed","trees, grass","trees","grass","tree","grassland","wood"].contains(primaryTagValue)) {
                priority = 12
                coeff_G = 1.0
                type = "vegetation"
            }
        }
        if (primaryTagKey == "landuse") {
            if (primaryTagValue == "reservoir" && secondaryTagKey == "covered" && secondaryTagValue == "no") {
                priority = 7
                coeff_G = 0.3
            }
            if (["residential","industrial","retail","harbour","quarry","landfill",
                 "construction","commercial","garages","railway","basin","farmyard"].contains(primaryTagValue)) {
                priority = 23
                coeff_G = 0.1
            }
            if (primaryTagValue == "brownfield") {
                priority = 24
                coeff_G = 0.3
            }
            if (primaryTagValue == "salt_pond") {
                priority = 25
                coeff_G = 0.5
            }
            if (["farmland","allotements","logging","plant_nursery","farm"].contains(primaryTagValue)) {
                priority = 26
                coeff_G = 0.7
            }
            if (["vineyard","orchard","greenfield","village_green"].contains(primaryTagValue)) {
                priority = 27
                coeff_G = 0.8
            }
            if (["meadow","forest","grass"].contains(primaryTagValue)) {
                priority = 28
                coeff_G = 1.0
                type = "vegetation"
            }
        }
        if (primaryTagKey == "leisure") {
            if (primaryTagValue == "pitch" && secondaryTagKey == "surface") {
                if (["asphalt","concrete","concrete:plate"].contains(secondaryTagValue)) {
                    priority = 1
                    coeff_G = 0.1
                }
                if (["dirt","compacted","sand","wood","clay"].contains(secondaryTagValue)) {
                    priority = 2
                    coeff_G = 0.2
                }
                if (["ground","fine_gravel","earth","mud"].contains(secondaryTagValue)) {
                    priority = 4
                    coeff_G = 0.5
                }
                if (["gravel"].contains(secondaryTagValue)) {
                    priority = 5
                    coeff_G = 0.7
                }
                if (["grass"].contains(secondaryTagValue)) {
                    priority = 6
                    coeff_G = 1.0
                    type = "vegetation"
                }
            }
            if (primaryTagValue == "marina") {
                priority = 30
                coeff_G = 0.2
            }
            if (primaryTagValue == "park") {
                priority = 31
                coeff_G = 0.7
                type = "park"
            }
            if (["garden","nature_reserve","golf_course"].contains(primaryTagValue)) {
                priority = 32
                coeff_G = 1.0
                type = "vegetation"
            }
        }
        if (primaryTagKey == "natural") {
            if (primaryTagValue == "beach" && secondaryTagKey == "surface") {
                if (secondaryTagValue == "sand") {
                    priority = 2
                    coeff_G = 0.2
                }
                if (secondaryTagValue == "shingle") {
                    priority = 3
                    coeff_G = 0.3
                }
                if (secondaryTagValue == "gravel" || secondaryTagValue == "pebbles") {
                    priority = 5
                    coeff_G = 0.7
                }
            }
            if (primaryTagValue == "wetland" && secondaryTagKey == "wetland") {
                if (secondaryTagValue == "tidalflat") {
                    priority = 14
                    coeff_G = 0.2
                }
                if (secondaryTagValue == "saltern") {
                    priority = 15
                    coeff_G = 0.3
                }
                if (secondaryTagValue == "marsh") {
                    priority = 16
                    coeff_G = 0.4
                }
                if (secondaryTagValue == "reebed") {
                    priority = 18
                    coeff_G = 0.6
                }
                if (secondaryTagValue == "bog") {
                    priority = 19
                    coeff_G = 0.7
                }
                if (secondaryTagValue == "mangrove") {
                    priority = 21
                    coeff_G = 1.0
                }
                if (["swamp","saltmarsh","wet_meadow"].contains(secondaryTagValue)) {
                    priority = 20
                    coeff_G = 0.9
                }
            }
            if (primaryTagValue == "bare_rock") {
                priority = 13
                coeff_G = 0.1
            }
            if (primaryTagValue == "bay") {
                priority = 7
                coeff_G = 0.3
            }
            if (primaryTagValue == "glacier") {
                priority = 13
                coeff_G = 0.1
            }
            if (primaryTagValue == "heath" || primaryTagValue == "grassland") {
                priority = 21
                coeff_G = 1.0
            }
            if (primaryTagValue == "rock") {
                priority = 13
                coeff_G = 0.1
            }
            if (primaryTagValue == "sand") {
                priority = 14
                coeff_G = 0.2
            }
            if (primaryTagValue == "scree") {
                priority = 17
                coeff_G = 0.5
            }
            if (primaryTagValue == "scrub") {
                priority = 19
                coeff_G = 0.7
            }
            if (primaryTagValue == "shingle") {
                priority = 15
                coeff_G = 0.3
            }
            if (primaryTagValue == "water") {
                priority = 7
                coeff_G = 0.3
            }
            if (primaryTagValue == "wood") {
                priority = 21
                coeff_G = 1.0
            }
        }
        if (primaryTagKey == "water") {
            if (["pond","lake","reservoir","river","wastewater","canal","oxbow",
                 "salt","lagoon","yes","ditch","salt_pool","tidal","stream",
                 "fishpond","riverbank","pool","lock","natural","shallow",
                 "salt_pond","lake;pond","marsh","well","reflecting_pool",
                 "fountain","stream;river","not_deep"].contains(primaryTagValue)) {
                priority = 7
                coeff_G = 0.3
            }
        }
        if (primaryTagKey == "waterway") {
            if (["stream","riverbank","canal","artificial"].contains(primaryTagValue)) {
                priority = 7
                coeff_G = 0.3
                type = "water"
            }
        }
    }

    void setGeom(Geometry geom) {
        this.geom = geom;
    }

}
