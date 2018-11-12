package ar.edu.itba.nosql;

import java.io.*;
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
import org.joda.time.DateTime;

public class GraphFramesPopulation {

    private static final int PARSE_TRJ_ID = 0;
    private static final int PARSE_TRJ_USER_ID = 1;
    private static final int PARSE_TRJ_VENUE_ID = 2;
    private static final int PARSE_TRJ_DATE = 3;
    private static final int PARSE_TRJ_TPOS = 4;

    private static final int PARSE_VNU_ID = 0;
    private static final int PARSE_VNU_CATEGORY = 1;
    private static final int PARSE_VNU_CATTYPE = 4;

    public static void main(String[] args) throws IOException {

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

    //population
    //loads both files (trayectories and venues) and returns vertex and edges
    private static Pair<Dataset<Row>, Dataset<Row>> Load(SQLContext sqlContext, JavaSparkContext sparkContext)
            throws IOException {

        List<Row> stops = new ArrayList<>();
        List<Row> venues = new ArrayList<>();
        List<Row> categories = new ArrayList<>();
        List<Row> category = new ArrayList<>();
        List<Row> trajStep = new ArrayList<>();
        List<Row> isVenue = new ArrayList<>();
        List<Row> hasCategory = new ArrayList<>();
        List<Row> subCategoryOf = new ArrayList<>();


        File file = new File("/user/maperazzo/prunned.tsv");

        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        long prevUserId = -1;
        long prevTrajId = -1;
        while ((st = br.readLine()) != null) {
            String[] splitted = st.split("\t");
            stops.add(RowFactory.create(Long.parseLong(splitted[PARSE_TRJ_ID]), Long.parseLong(splitted[PARSE_TRJ_USER_ID]),
                    DateTime.parse(splitted[PARSE_TRJ_DATE]), Long.parseLong(splitted[PARSE_TRJ_TPOS])));
            isVenue.add(RowFactory.create(Long.parseLong(splitted[PARSE_TRJ_USER_ID]), splitted[PARSE_TRJ_VENUE_ID],
                    "isVenue"));

            Long currentUserId = Long.parseLong(splitted[PARSE_TRJ_USER_ID]);
            Long currentTrajId = Long.parseLong(splitted[PARSE_TRJ_ID]);
            if (prevUserId != -1 && prevUserId == currentUserId)
                trajStep.add(RowFactory.create(prevTrajId, currentTrajId, "trajStep"));

            prevUserId = currentUserId;
            prevTrajId = currentTrajId;
        }

        file = new File("/user/maperazzo/categories.tsv");

        br = new BufferedReader(new FileReader(file));

        while ((st = br.readLine()) != null) {
            String[] splitted = st.split("\t");
            //Nodes
            venues.add(RowFactory.create(splitted[PARSE_VNU_ID]));
            categories.add(RowFactory.create(splitted[PARSE_VNU_CATEGORY]));
            category.add(RowFactory.create(splitted[PARSE_VNU_CATTYPE]));
            //Relations
            hasCategory.add(RowFactory.create(splitted[PARSE_VNU_ID], splitted[PARSE_VNU_CATEGORY], "hasCategory"));
            subCategoryOf.add(RowFactory.create(splitted[PARSE_VNU_CATEGORY], splitted[PARSE_VNU_CATTYPE], "subCategoryOf"));
        }

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

    // metadata
    public static StructType CreateVertexStopSchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();
        vertFields.add(DataTypes.createStructField("id",DataTypes.LongType, false));
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
