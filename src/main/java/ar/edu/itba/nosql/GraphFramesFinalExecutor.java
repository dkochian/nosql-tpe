package ar.edu.itba.nosql;

import org.apache.commons.math3.util.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;

public class GraphFramesFinalExecutor {

    private static final String FILE_NAME_INPUT = "mapping";
    private static final Integer QUERY_NUMBER = 1;

    public static void main(String[] args) {

        SparkSession sp = SparkSession.builder().appName("FinalQueryExecutor").getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sp.sparkContext());
        SQLContext sqlContext = new SQLContext(sp);

        Pair<Dataset<Row>, Dataset<Row>> nodesAndEdges = LoadNodesAndEdges(sqlContext);

        GraphFrame graph = GraphFrame.apply(nodesAndEdges.getKey(), nodesAndEdges.getValue());

        final long start = System.currentTimeMillis();

        if (QUERY_NUMBER == 1)
            Query1(graph).show(Integer.MAX_VALUE);
        else if (QUERY_NUMBER == 2)
            Query2(graph).show(Integer.MAX_VALUE);

        System.out.println("System.currentTimeMillis() - start = " + (System.currentTimeMillis() - start));

        sparkContext.close();
    }

    private static Dataset<Row> Query1(GraphFrame graph) {
        return graph.find("(s)")
                .filter("s.label='Stop'")
                .groupBy("s.userId")
                .agg(max("s.tpos").alias("length"))
                .select("userId", "length");

    }

    private static Dataset<Row> Query2(GraphFrame graph) {
        return graph.find("(s)-[e1]->(v); (v)-[e2]->(cat); (cat)-[e3]->(c)")
                .filter("e1.label='isVenue' and e2.label='hasCategory' and e3.label='subCategoryOf'")
                .filter("s.label='Stop' and v.label='Venue' and cat.label='Categories' and c.label='Category'")
                .filter("c.secondId='Airport'")
                .groupBy("s.userId")
                .agg(count("s.id").alias("cantidad"))
                .select("userId", "cantidad");
    }

    private static Dataset<Row> QueryTesting(GraphFrame graph) {
        return graph.find("(s)")
                .filter("s.label='Stop'")
                .agg(count("s.id").alias("count"))
                .select("count");
    }

    private static Dataset<Row> QueryTesting2(GraphFrame graph) {
        return graph.find("(s)")
                .filter("s.label='Stop'")
                .select( "s");
    }


    private static Pair<Dataset<Row>, Dataset<Row>> LoadNodesAndEdges(SQLContext sqlContext) {
        Dataset<Row> nodes = sqlContext.read().parquet("hdfs:///user/maperazzo/" + FILE_NAME_INPUT + "_nodes");
        Dataset<Row> edges = sqlContext.read().parquet("hdfs:///user/maperazzo/" + FILE_NAME_INPUT + "_edges");
        return new Pair<>(nodes, edges);
    }
}
