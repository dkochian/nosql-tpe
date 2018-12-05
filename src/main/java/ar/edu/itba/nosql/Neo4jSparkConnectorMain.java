package ar.edu.itba.nosql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.neo4j.spark.*;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.LinkedList;
import java.util.List;


public class Neo4jSparkConnectorMain {

 	public static void main (String[] args){
        SparkSession sp = SparkSession.builder().appName("Neo4j Connector")
                .config("spark.neo4j.bolt.url","bolt://node1.it.itba.edu.ar:7689")
                .config("spark.neo4j.bolt.user","jdantur")
                .config("spark.neo4j.bolt.password","jdantur")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sp.sparkContext());

		Neo4j neo = new Neo4j(jsc.sc());


		Neo4j stops = neo.cypher("MATCH (s:Stop) RETURN s.id as id, null as secondId, s.userId as userId, s.utctimestamp as utctimestamp, s.tpos as tpos, labels(s) as label",
				new scala.collection.immutable.HashMap<>());

		Neo4j venues = neo.cypher("MATCH (v:Venue) RETURN id(v) as id, v.id as secondId, null as userId, null as utctimestamp, null as tpos, labels(v) as label",
				new scala.collection.immutable.HashMap<>());

		Neo4j categories = neo.cypher("MATCH (c:Categories) RETURN id(c) as id, c.name as secondId, null as userId, null as utctimestamp, null as tpos, labels(c) as label",
				new scala.collection.immutable.HashMap<>());

		Neo4j category = neo.cypher("MATCH (c:Category) RETURN id(c) as id, c.name as secondId, null as userId, null as utctimestamp, null as tpos, labels(c) as label",
				new scala.collection.immutable.HashMap<>());

		//Neo4j nodes = neo.cypher("MATCH (n) RETURN id(n) as id ", new scala.collection.immutable.HashMap<>());

		Dataset<Row> stopsdf = stops.loadDataFrame();

		Dataset<Row> venuesdf = venues.loadDataFrame();

		Dataset<Row> categoriesdf = categories.loadDataFrame();

		Dataset<Row> categorydf = category.loadDataFrame();


		Neo4j edges = neo.cypher("MATCH (n)-[x]->(n1) RETURN id(n) as src, type(x) as label, id(n1) as dst ",
				new scala.collection.immutable.HashMap<>());

		Dataset<Row> edgesdf = edges.loadDataFrame();

		GraphFrame graph = GraphFrame.apply(stopsdf.union(venuesdf).union(categoriesdf).union(categorydf), edgesdf);

//		final Dataset<Row> query1 = graph.find("(s1)-[e11]->(v1); (v1)-[e12]->(cat1); (cat1)-[e13]->(c1); " +
//				"(s2)-[e21]->(v2); (v2)-[e22]->(cat2); (cat2)-[e23]->(c2); " +
//				"(s3)-[e31]->(v3); (v3)-[e32]->(cat3); (cat3)-[e33]->(c3); " +
//				"(s1)-[e1]->(s2); (s2)-[e2]->(s3)")
//				.filter("e11.label='isVenue' and e21.label='isVenue' and e31.label='isVenue'")
//				.filter("e12.label='hasCategory' and e22.label='hasCategory' and e32.label='hasCategory'")
//				.filter("e13.label='subCategoryOf' and e23.label='subCategoryOf' and e33.label='subCategoryOf'")
//				.filter("e1.label='trajStep' and e2.label='trajStep'")
//				.filter("s1.label='Stop' and s2.label='Stop' and s3.label='Stop'")
//				.filter("v1.label='Venue' and v2.label='Venue' and v3.label='Venue'")
//				.filter("cat1.label='Categories' and cat2.label='Categories' and cat3.label='Categories'")
//				.filter("c1.label='Category' and c2.label='Category' and c3.label='Category'")
//				.filter("c1.secondId='Home' and c2.secondId='Station' and     c3.secondId='Airport'")
//				.distinct()
//				.groupBy("s1.userId")
//				.agg(collect_list("s1.tpos").alias("from"))
//				.select("userId", "from");

//		rta.vertices().printSchema();
//		rta.vertices().show(Integer.MAX_VALUE);
//
//		rta.edges().printSchema();
//		rta.edges().show(Integer.MAX_VALUE);

//		query1.show(Integer.MAX_VALUE);


        jsc.close();

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
