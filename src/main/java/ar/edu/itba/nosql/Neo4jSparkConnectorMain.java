package ar.edu.itba.nosql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.neo4j.spark.*;
import scala.Tuple2;
import scala.collection.Seq;

import javax.xml.crypto.Data;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.functions.count;


public class Neo4jSparkConnectorMain {

	private static final String FILE_NAME_OUTPUT = "mapping";

	public static void main (String[] args){
        SparkSession sp = SparkSession.builder().appName("Neo4j Connector")
                .config("spark.neo4j.bolt.url","bolt://node1.it.itba.edu.ar:9689")
                .config("spark.neo4j.bolt.user","jdantur")
                .config("spark.neo4j.bolt.password","jdantur")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sp.sparkContext());

		Neo4j neo = new Neo4j(jsc.sc());


		Neo4j stops = neo.cypher("MATCH (s:Stop) RETURN s.id as id, null as secondId, s.userId as userId, s.utctimestamp as utctimestamp, s.tpos as tpos, labels(s)(1) as label",
				new scala.collection.immutable.HashMap<>());

		Neo4j venues = neo.cypher("MATCH (v:Venue) RETURN id(v) as id, v.id as secondId, null as userId, null as utctimestamp, null as tpos, labels(v)(1) as label",
				new scala.collection.immutable.HashMap<>());

		Neo4j categories = neo.cypher("MATCH (c:Categories) RETURN id(c) as id, c.name as secondId, null as userId, null as utctimestamp, null as tpos, labels(c)(1) as label",
				new scala.collection.immutable.HashMap<>());

		Neo4j category = neo.cypher("MATCH (c:Category) RETURN id(c) as id, c.name as secondId, null as userId, null as utctimestamp, null as tpos, labels(c)(1) as label",
				new scala.collection.immutable.HashMap<>());

		Dataset<Row> nodesdf = stops.loadDataFrame(GraphFramesSchema()).union(venues.loadDataFrame(GraphFramesSchema()))
				.union(categories.loadDataFrame(GraphFramesSchema()))
				.union(category.loadDataFrame(GraphFramesSchema()));

		Neo4j edges = neo.cypher("MATCH (n)-[x]->(n1) RETURN id(n) as src, type(x) as label, id(n1) as dst ",
				new scala.collection.immutable.HashMap<>());

		Dataset<Row> edgesdf = edges.loadDataFrame();

		GraphFrame graph = GraphFrame.apply(nodesdf, edgesdf);

		graph.vertices().show(Integer.MAX_VALUE);
		graph.edges().show(Integer.MAX_VALUE);
		//Query1(graph).show(Integer.MAX_VALUE);
		//write(nodesdf, edgesdf);

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

	private static void write (Dataset<Row> nodes, Dataset<Row> edges) {
		nodes.write().mode(SaveMode.Overwrite).parquet("hdfs:///user/maperazzo/" + FILE_NAME_OUTPUT + "_nodes");
		edges.write().mode(SaveMode.Overwrite).parquet("hdfs:///user/maperazzo/" + FILE_NAME_OUTPUT + "_edges");
	}

	private static Dataset<Row> Query1(GraphFrame graph) {

		final Dataset<Row> query1 = graph.find("(s)-[e1]->(v); (v)-[e2]->(cat); (cat)-[e3]->(c)")
				.filter("e1.label='isVenue' and e2.label='hasCategory' and e3.label='subCategoryOf'")
				.filter("s.label='Stop' and v.label='Venue' and cat.label='Categories' and c.label='Category'")
				.filter("c.secondId='Airport'")
				.groupBy("s.userId")
				.agg(count("s.id").alias("cantidad"))
				.select("userId", "cantidad");

		return query1;
	}
}
