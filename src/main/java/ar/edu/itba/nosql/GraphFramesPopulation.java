package ar.edu.itba.nosql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.clearspring.analytics.util.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class GraphFramesPopulation {

    public static void main(String[] args) throws ParseException {

        SparkSession sp = SparkSession.builder().appName("Population").getOrCreate();
        JavaSparkContext sparkContext= new JavaSparkContext(sp.sparkContext());
        SQLContext sqlContext = new SQLContext(sp);

        Pair<Dataset<Row>, Dataset<Row>> graph = Load(sqlContext, sparkContext);
        // create the graph
        GraphFrame myGraph = GraphFrame.apply(graph.left, graph.right);

        // in the driver
        myGraph.vertices().show();
        myGraph.edges().show();

        sparkContext.close();
    }

    //loads both files (trayectories and venues) and returns vertex and edges
    private static Pair<Dataset<Row>, Dataset<Row>> Load(SQLContext sqlContext, JavaSparkContext sparkContext) {

        List<Row> stops = new ArrayList<>();
        List<Row> venues = new ArrayList<>();
        List<Row> categories = new ArrayList<>();
        List<Row> category = new ArrayList<>();
        List<Row> trajStep = new ArrayList<>();
        List<Row> isVenue = new ArrayList<>();
        List<Row> hasCategory = new ArrayList<>();
        List<Row> subCategoryOf = new ArrayList<>();


        Dataset<Row> stopsDF =
                sqlContext.createDataFrame(sparkContext.parallelize(stops), CreateVertexStopSchema());

        Dataset<Row> venuesDF =
                sqlContext.createDataFrame(sparkContext.parallelize(venues), CreateVertexVenueSchema());

        Dataset<Row> categoriesDF =
                sqlContext.createDataFrame(sparkContext.parallelize(categories), CreateVertexCategoriesSchema());

        Dataset<Row> categoryDF =
                sqlContext.createDataFrame(sparkContext.parallelize(category), CreateVertexCategorySchema());

        Dataset<Row> trajStepDF =
                sqlContext.createDataFrame(sparkContext.parallelize(trajStep), CreateEdgetrajStepSchema());

        Dataset<Row> isVenueDF =
                sqlContext.createDataFrame(sparkContext.parallelize(isVenue), CreateEdgeisVenueSchema());

        Dataset<Row> hasCategoryDF =
                sqlContext.createDataFrame(sparkContext.parallelize(hasCategory), CreateEdgehasCategorySchema());

        Dataset<Row> subCategoryOfDF =
                sqlContext.createDataFrame(sparkContext.parallelize(subCategoryOf), CreateEdgesubCategoryOfSchema());

        return new Pair<>(stopsDF.union(venuesDF).union(categoriesDF).union(categoryDF),
                trajStepDF.union(isVenueDF).union(hasCategoryDF).union(subCategoryOfDF));
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

    // metadata
    public static StructType CreateVertexStopSchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("userid",DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("utctimestamp",DataTypes.DateType, false));
        vertFields.add(DataTypes.createStructField("tpos",DataTypes.LongType, false));

        return DataTypes.createStructType(vertFields);
    }

    // metadata
    public static StructType CreateVertexVenueSchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("venueid",DataTypes.LongType, false));

        return DataTypes.createStructType(vertFields);
    }

    // metadata
    public static StructType CreateVertexCategoriesSchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("venuecategory",DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    // metadata
    public static StructType CreateVertexCategorySchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();

        vertFields.add(DataTypes.createStructField("cattype",DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    // metadata
    public static StructType CreateEdgeSchema(final DataType src, final DataType dest)
    {
        List<StructField> edgeFields = new ArrayList<StructField>();

        edgeFields.add(DataTypes.createStructField("src", src, false));
        edgeFields.add(DataTypes.createStructField("dst", dest, false));
        edgeFields.add(DataTypes.createStructField("label", DataTypes.StringType, false));

        return DataTypes.createStructType(edgeFields);
    }

    // metadata
    public static StructType CreateEdgetrajStepSchema()
    { return CreateEdgeSchema(DataTypes.LongType, DataTypes.LongType); }

    // metadata
    public static StructType CreateEdgeisVenueSchema()
    { return CreateEdgeSchema(DataTypes.LongType, DataTypes.StringType); }

    // metadata
    public static StructType CreateEdgehasCategorySchema()
    { return CreateEdgeSchema(DataTypes.StringType, DataTypes.StringType);}

    // metadata
    public static StructType CreateEdgesubCategoryOfSchema()
    { return CreateEdgeSchema(DataTypes.StringType, DataTypes.StringType); }
}
