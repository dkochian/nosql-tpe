package ar.edu.itba.nosql;

import java.util.Collections;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.neo4j.spark.*;

import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

public class Neo4jSparkConnectorMain {

 	public static void main (String[] args){
        SparkSession sp = SparkSession.builder().appName("Neo4j Test")
                .config("spark.neo4j.bolt.url","bolt://node1.it.itba.edu.ar:7689")
                .config("spark.neo4j.bolt.user","jdantur")
                .config("spark.neo4j.bolt.password","jdantur")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sp.sparkContext());
        
        Neo4j neo = new Neo4j(jsc.sc());
  
		
    	Map<String, Object> params= new HashMap<>();
    	
		Neo4j nodes = neo.cypher("MATCH (n) RETURN id(n) as id ", params );
		Dataset<Row> nodesdf = nodes.loadDataFrame();
		
		
		Neo4j edges = neo.cypher("MATCH (n)-[x]->(n1) RETURN id(n) as src, type(x) as mylabel, id(n1) as dst ", params );
		Dataset<Row> edgesdf = edges.loadDataFrame();
		
		GraphFrame rta = GraphFrame.apply(nodesdf, edgesdf);
		
		
		// bla bla bla
		
		rta.vertices().printSchema();
		rta.vertices().show();
		
		rta.edges().printSchema();
		rta.edges().show();
		

        jsc.close();

    }
}
