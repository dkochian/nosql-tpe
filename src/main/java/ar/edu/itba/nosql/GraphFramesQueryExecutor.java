package ar.edu.itba.nosql;


import org.apache.commons.math3.util.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.udf;


public class GraphFramesQueryExecutor {

    private static final String FILE_NAME_INPUT = "Trajectories_testing";

    public static void main(String[] args) {

        SparkSession sp = SparkSession.builder().appName("QueryExecutor").getOrCreate();
        JavaSparkContext sparkContext= new JavaSparkContext(sp.sparkContext());
        SQLContext sqlContext = new SQLContext(sp);

        Pair<Dataset<Row>, Dataset<Row>> nodesAndEdges = LoadNodesAndEdges(sqlContext);

        GraphFrame graph = GraphFrame.apply(nodesAndEdges.getKey(), nodesAndEdges.getValue());

        Query4(graph).show(Integer.MAX_VALUE);

        sparkContext.close();
    }

    private static Pair<Dataset<Row>, Dataset<Row>> LoadNodesAndEdges(SQLContext sqlContext) {
        Dataset<Row> nodes = sqlContext.read().parquet("hdfs:///user/maperazzo/" + FILE_NAME_INPUT + "_nodes");
        Dataset<Row> edges = sqlContext.read().parquet("hdfs:///user/maperazzo/" + FILE_NAME_INPUT + "_edges");
        return new Pair<>(nodes, edges);
    }

    private static Dataset<Row> Query1(GraphFrame graph) {
        Dataset<Row> query1 = graph.find("(s1)-[e11]->(v1); (v1)-[e12]->(cat1); (cat1)-[e13]->(c1); " +
                "(s2)-[e21]->(v2); (v2)-[e22]->(cat2); (cat2)-[e23]->(c2); " +
                "(s3)-[e31]->(v3); (v3)-[e32]->(cat3); (cat3)-[e33]->(c3); " +
                "(s1)-[e1]->(s2); (s2)-[e2]->(s3)")
                .filter("e11.label='isVenue' and e21.label='isVenue' and e31.label='isVenue'")
                .filter("e12.label='hasCategory' and e22.label='hasCategory' and e32.label='hasCategory'")
                .filter("e13.label='subCategoryOf' and e23.label='subCategoryOf' and e33.label='subCategoryOf'")
                .filter("e1.label='trajStep' and e2.label='trajStep'")
                .filter("s1.label='Stop' and s2.label='Stop' and s3.label='Stop'")
                .filter("v1.label='Venues' and v2.label='Venues' and v3.label='Venues'")
                .filter("cat1.label='Categories' and cat2.label='Categories' and cat3.label='Categories'")
                .filter("c1.label='Category' and c2.label='Category' and c3.label='Category'")
                .filter("c1.secondId='Home' and c2.secondId='Station' and c3.secondId='Airport'")
                .filter("e12.label='hasCategory' and e21.label='hasCategory' and e31.label='hasCategory'")
                .distinct()
                .groupBy("s1.userId")
                .agg(collect_list("s1.tpos").alias("from"))
                .select("userId", "from");

        return query1;
    }

    private static Dataset<Row> Query2(GraphFrame graph) {
        Dataset<Row> start = graph.find("(s1)-[e11]->(v1); (v1)-[e12]->(c1); (c1)-[e13]->(cs1)")
                .filter("s1.label='Stop'")
                .filter("e11.label='isVenue'")
                .filter("v1.label='Venues'")
                .filter("c1.label='Categories'")
                .filter("cs1.label='Category'")
                .filter("cs1.secondId='Home'")
                .selectExpr("s1.userId","s1.utctimestamp as timestart","s1.tpos as posstart")
                .distinct();
        Dataset<Row> end = graph.find("(s1)-[e11]->(v1); (v1)-[e12]->(c1); (c1)-[e13]->(cs1)")
                .filter("s1.label='Stop'")
                .filter("v1.label='Venues'")
                .filter("c1.label='Categories'")
                .filter("cs1.label='Category'")
                .filter("cs1.secondId='Airport'")
                .selectExpr("s1.userId","s1.utctimestamp as timeend","s1.tpos as posend")
                .distinct();
        return start.join(end,"userId")
                .filter("posend>posstart")
                .filter("timestart=timeend");

    }

    private static Dataset<Row> Query4(GraphFrame graph) {

        Dataset<Row> start = graph.vertices().filter("label = 'Stop'")
                .selectExpr("userId","utctimestamp as timestart","tpos as posstart");
        //start.show(Integer.MAX_VALUE);
        Dataset<Row> end = graph.vertices().filter("label = 'Stop'")
                .selectExpr("userId","utctimestamp as timeend","tpos as posend");
        //end.show(Integer.MAX_VALUE);
        Dataset<Row> longestLength = start.join(end,"userId")
                .filter("posend>posstart")
                .filter("timestart=timeend")
                .selectExpr("userId","posend-posstart as length")
                .groupBy("userId")
                .agg(max("length").alias("maxLength"));
        //longestLength.show(Integer.MAX_VALUE);
        return start.join(end,"userId")
                .filter("posend>posstart")
                .filter("timestart=timeend")
                .selectExpr("userId","timestart as time","posstart","posend","posend-posstart as length")
                .join(longestLength,"userId")
                .filter("length>=maxLength")
                .selectExpr("userId","time","posstart","posend");




    }
}
