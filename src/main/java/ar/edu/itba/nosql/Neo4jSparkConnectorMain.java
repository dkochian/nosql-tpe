package ar.edu.itba.nosql;

import org.apache.spark.sql.SparkSession;
import org.neo4j.spark.*;

public class Neo4jSparkConnectorMain {

    public static void main (String[] args){
        SparkSession sp = SparkSession.builder().appName("Neo4j Test")
                .config("spark.neo4j.bolt.url","bolt://node1.it.itba.edu.ar:7689")
                .config("spark.neo4j.bolt.user","jdantur")
                .config("spark.neo4j.bolt.password","jdantur")
                .getOrCreate();
        Neo4JavaSparkContext nsc = Neo4JavaSparkContext.neo4jContext(sp.sparkContext());
        nsc.queryDF("MATCH (n1)-[r]->(n2) RETURN r, n1, n2;",null).show(Integer.MAX_VALUE);

    }
}
