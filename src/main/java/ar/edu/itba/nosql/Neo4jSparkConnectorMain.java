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


public class Neo4jSparkConnectorMain {

	private static final String FILE_NAME_OUTPUT = "mapping";

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

		Dataset<Row> nodesdf = stops.loadDataFrame().union(venues.loadDataFrame())
				.union(categories.loadDataFrame())
				.union(category.loadDataFrame());

		Neo4j edges = neo.cypher("MATCH (n)-[x]->(n1) RETURN id(n) as src, type(x) as label, id(n1) as dst ",
				new scala.collection.immutable.HashMap<>());

		Dataset<Row> edgesdf = edges.loadDataFrame();

		write(nodesdf, edgesdf);

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
}
