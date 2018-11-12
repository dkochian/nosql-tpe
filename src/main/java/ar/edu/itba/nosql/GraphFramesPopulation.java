package ar.edu.itba.nosql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class GraphFramesPopulation {

    public static void main(String[] args) throws ParseException {

        // SparkConf spark = new SparkConf().setAppName("Population);
        SparkSession sp = SparkSession.builder().appName("Population").getOrCreate();
        JavaSparkContext sparkContext= new JavaSparkContext(sp.sparkContext());
        SQLContext sqlContext = new SQLContext(sp);

        // parallelize vertices and edges values
        Dataset<Row> verticesDF =
                sqlContext.createDataFrame(sparkContext.parallelize(LoadVertices()), CreateVertexSchema() );


        Dataset<Row> edgesDF =
                sqlContext.createDataFrame(sparkContext.parallelize(LoadEdges()), CreateEdgeSchema() );


        // create the graph
        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);

        // in the driver
        myGraph.vertices().show();
        myGraph.edges().show();

        sparkContext.close();

    }




    // metadata
    public static StructType CreateVertexSchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("URL",DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("owner",DataTypes.StringType, true));

        return DataTypes.createStructType(vertFields);
    }


    //population
    public static ArrayList<Row> LoadVertices()
    {
        ArrayList<Row> vertList = new ArrayList<Row>();

        for(long rec= 0; rec <= 9; rec++)
        {
            vertList.add(RowFactory.create( rec, rec  + ".html", rec%2==0?"A":"L"));
        }

        return vertList;
    }

    // metadata
    public static StructType CreateEdgeSchema()
    {
        List<StructField> edgeFields = new ArrayList<StructField>();

        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, true));
        edgeFields.add(DataTypes.createStructField("label",DataTypes.StringType, true));
        edgeFields.add(DataTypes.createStructField("date",DataTypes.DateType, true));

        return DataTypes.createStructType(edgeFields);
    }

    //population
    public static  ArrayList<Row> LoadEdges() throws ParseException
    {
        ArrayList<Row> edges = new ArrayList<Row>();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");


        edges.add(RowFactory.create( 1L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime() )));
        edges.add(RowFactory.create( 1L, 2L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 2L, 0L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 3L, 2L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 3L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));
        edges.add(RowFactory.create( 4L, 3L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));
        edges.add(RowFactory.create( 5L, 4L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime())));
        edges.add(RowFactory.create( 6L, 4L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 7L, 6L, "refersTo", new java.sql.Date(sdf.parse("10/10/2010").getTime())));
        edges.add(RowFactory.create( 7L, 8L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));
        edges.add(RowFactory.create( 8L, 5L, "refersTo", new java.sql.Date(sdf.parse("21/10/2010").getTime())));
        edges.add(RowFactory.create( 8L, 6L, "refersTo", new java.sql.Date(sdf.parse("15/10/2010").getTime())));

        return edges;
    }



}
