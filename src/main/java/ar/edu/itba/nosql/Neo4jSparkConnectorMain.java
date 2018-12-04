package ar.edu.itba.nosql;

import java.util.*;

import ar.edu.itba.nosql.utils.Converter;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.neo4j.spark.*;
import scala.Tuple2;
import scala.collection.Seq;


public class Neo4jSparkConnectorMain {

	private static final int PARSE_TRJ_ID = 0;
	private static final int PARSE_TRJ_USER_ID = 1;
	private static final int PARSE_TRJ_VENUE_ID = 2;
	private static final int PARSE_TRJ_DATE = 3;
	private static final int PARSE_TRJ_TPOS = 4;

	private static final int PARSE_VNU_ID = 0;
	private static final int PARSE_VNU_CATEGORY = 1;
	private static final int PARSE_VNU_CATTYPE = 4;

	private static final Set<String> categories = new HashSet<>();
	private static final Set<String> cattypes = new HashSet<>();

	private static boolean populate = true;

	private static Converter converter = new Converter();

 	public static void main (String[] args){
        SparkSession sp = SparkSession.builder().appName("Neo4j Test")
                .config("spark.neo4j.bolt.url","bolt://node1.it.itba.edu.ar:7689")
                .config("spark.neo4j.bolt.user","jdantur")
                .config("spark.neo4j.bolt.password","jdantur")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sp.sparkContext());
		SQLContext sqlContext = new SQLContext(sp);


		Neo4j neo = new Neo4j(jsc.sc());

		if (populate) {
			//clear DB
			neo.cypher("MATCH (n)\n" +
							"DETACH DELETE n", new scala.collection.immutable.HashMap<>());

			Pair<Dataset<Row>, Dataset<Row>> files = GraphFramesPopulation.LoadVenuesAndTrajectories(sqlContext);

			PopulateUsingVenues(neo, files.getValue());
			PopulateUsingTrajectories(neo, files.getKey());
		}

		Neo4j nodes = neo.cypher("MATCH (s:Stop) RETURN s.id as id, null as secondId, s.userId as userId, s.utctimestamp as utctimestamp, s.tpos as tpos, labels(s) as label\n" +
						"UNION\n" +
						"MATCH (v:Venue) RETURN id(v) as id, v.id as secondId, null as userId, null as utctimestamp, null as tpos, labels(v) as label\n" +
						"UNION\n" +
						"MATCH (c:Categories) RETURN id(c) as id, c.name as secondId, null as userId, null as utctimestamp, null as tpos, labels(c) as label\n" +
						"UNION\n" +
						"MATCH (c:Category) RETURN id(c) as id, c.name as secondId, null as userId, null as utctimestamp, null as tpos, labels(c) as label",
				new scala.collection.immutable.HashMap<>());

		Dataset<Row> nodesdf = nodes.loadDataFrame(GraphFramesSchema());

		Neo4j edges = neo.cypher("MATCH (n)-[x]->(n1) RETURN id(n) as src, type(x) as label, id(n1) as dst ",
				new scala.collection.immutable.HashMap<>());

		Dataset<Row> edgesdf = edges.loadDataFrame();

		GraphFrame rta = GraphFrame.apply(nodesdf, edgesdf);

		rta.vertices().printSchema();
		rta.vertices().show();

		rta.edges().printSchema();
		rta.edges().show();


        jsc.close();

    }

	private static final void PopulateUsingVenues(final Neo4j neo, final Dataset<Row> venues) {

		for (Row v : venues.collectAsList()) {
			scala.collection.immutable.Map params = PopulateVenueParams(v);

			neo.cypher("CREATE (n:Venue { id: venueId })", params);

			if (!categories.contains(v.getString(PARSE_VNU_CATEGORY))) {
				categories.add(v.getString(PARSE_VNU_CATEGORY));
				neo.cypher("CREATE (n:Categories { name: category })", params);

				if (!cattypes.contains(v.getString(PARSE_VNU_CATTYPE))) {
					cattypes.add(v.getString(PARSE_VNU_CATTYPE));
					neo.cypher("CREATE (n:Category { name: cattype })", params);
				}

				neo.cypher("MATCH (c1:Categories),(c2:Category)\n" +
						"WHERE c1.name = category and c2.name = cattype\n" +
						"CREATE (c1)-[r:subCategoryOf]->(c2)\n", params);
			}

			neo.cypher("MATCH (v:Venue),(c:Categories)\n" +
					"WHERE v.id = venueId and c.name = category\n" +
					"CREATE (v)-[r:hasCategory]->(c)\n", params);
		}
	}

	private static scala.collection.immutable.Map PopulateVenueParams(Row v) {
		Map<String, Object> params = new HashMap<>();
		params.put("venueId", v.getString(PARSE_VNU_ID));
		params.put("category", v.getString(PARSE_VNU_CATEGORY));
		params.put("cattype", v.getString(PARSE_VNU_CATTYPE));

		return converter.convert(params);
	}

	private static final void PopulateUsingTrajectories(final Neo4j neo, final Dataset<Row> trajectories) {
		long prevUserId = -1L;
		long prevTrajId = -1L;
		for (Row t : trajectories.collectAsList()) {
			scala.collection.immutable.Map params = PopulateTrajectoryParams(t, prevTrajId);

			neo.cypher("CREATE (n:Stop { id: trjId, userId: userId, utctimestamp: date, tpos: tpos })", params);

			neo.cypher("MATCH (s:Stop),(v:Venue)\n" +
					"WHERE s.id = trjId and v.id = venueId\n" +
					"CREATE (s)-[r:isVenue]->(v)\n", params);

			if (prevUserId == t.getLong(PARSE_TRJ_USER_ID))
				neo.cypher("MATCH (s1:Stop),(s2:Stop)\n" +
						"WHERE s1.id = prevTrjId and s2.id = trjId\n" +
						"CREATE (s1)-[r:trajStep]->(s2)\n", params);


			prevUserId = t.getLong(PARSE_TRJ_USER_ID);
			prevTrajId = t.getLong(PARSE_TRJ_ID);

		}
	}

	private static scala.collection.immutable.Map PopulateTrajectoryParams(Row t, long prevTrajId) {
		Map<String, Object> params = new HashMap<>();
		params.put("trjId", t.getLong(PARSE_TRJ_ID));
		params.put("userId", t.getLong(PARSE_TRJ_USER_ID));
		params.put("date", t.getDate(PARSE_TRJ_DATE).toString());
		params.put("tpos", t.getLong(PARSE_TRJ_TPOS));
		params.put("venueId", t.getString(PARSE_TRJ_VENUE_ID));
		params.put("prevTrjId", prevTrajId);

		return converter.convert(params);

	}

	private static Seq<Tuple2<String, String>> GraphFramesSchema() {
		List<Tuple2<String, String>> schema = new LinkedList<>();
		schema.add(new Tuple2<>("id", "long"));
		schema.add(new Tuple2<>("secondId", "string"));
		schema.add(new Tuple2<>("userId", "long"));
		schema.add(new Tuple2<>("utctimestamp", "string"));
		schema.add(new Tuple2<>("tpos", "long"));
		schema.add(new Tuple2<>("label", "string"));
		return scala.collection.JavaConversions.asScalaBuffer(schema).toSeq();
	}
}
