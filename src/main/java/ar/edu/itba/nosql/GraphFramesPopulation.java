package ar.edu.itba.nosql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
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

    private static final int PARSE_TRJ_ID = 0;
    private static final int PARSE_TRJ_USER_ID = 1;
    private static final int PARSE_TRJ_VENUE_ID = 2;
    private static final int PARSE_TRJ_DATE = 3;
    private static final int PARSE_TRJ_TPOS = 4;

    private static final int PARSE_VNU_ID = 0;
    private static final int PARSE_VNU_CATEGORY = 1;
    private static final int PARSE_VNU_CATTYPE = 4;

    private static long venuesIdMax = 1;
    private static long categoriesIdMax = 1;
    private static long cattypeIdMax = 1;

    private static final Map<String, Long> venuesId = new HashMap<>();
    private static final Map<String, Long> categoriesId = new HashMap<>();
    private static final Map<String, Long> cattypeId = new HashMap<>();



    public static void main(String[] args) {

        SparkSession sp = SparkSession.builder().appName("Population").getOrCreate();
        JavaSparkContext sparkContext= new JavaSparkContext(sp.sparkContext());
        SQLContext sqlContext = new SQLContext(sp);

        Pair<Dataset<Row>, Dataset<Row>> files = LoadVenuesAndTrajectories(sqlContext);
        
        List<Row> nodes = new ArrayList<>();
        List<Row> edges = new ArrayList<>();

        PopulateUsingTrajectories(nodes, edges, files.getKey());

        PopulateUsingVenues(nodes, edges, files.getValue());

        Dataset<Row> nodesDF =
                sqlContext.createDataFrame(sparkContext.parallelize(nodes), CreateVertexSchema());

        Dataset<Row> edgesDF =
                sqlContext.createDataFrame(sparkContext.parallelize(edges), CreateEdgeSchema());

        // create the graph
        GraphFrame myGraph = GraphFrame.apply(nodesDF, edgesDF);

        // in the driver
        myGraph.vertices().show();
        myGraph.edges().show();

        sparkContext.close();
    }

    private static Pair<Dataset<Row>, Dataset<Row>> LoadVenuesAndTrajectories(SQLContext sqlContext) {

        StructType trajectorySchema = new StructType(new StructField[] {
                DataTypes.createStructField("id",DataTypes.LongType, false),
                DataTypes.createStructField("userid",DataTypes.LongType, false),
                DataTypes.createStructField("venueid",DataTypes.StringType, false),
                DataTypes.createStructField("date",DataTypes.DateType, false),
                DataTypes.createStructField("tpos",DataTypes.LongType, false)});

        StructType venueSchema = new StructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("category",DataTypes.StringType, false),
                DataTypes.createStructField("longitude",DataTypes.DoubleType, false),
                DataTypes.createStructField("latitude",DataTypes.DoubleType, false),
                DataTypes.createStructField("cattype",DataTypes.StringType, false)});

        Dataset<Row> trajectories = sqlContext.read().format("csv").option("delimiter","\t").option("header", "true")
                .schema(trajectorySchema)
                .load("hdfs:///user/maperazzo/prunned.tsv");

        Dataset<Row> venues = sqlContext.read().format("csv").option("delimiter","\t").option("header", "true")
                .schema(venueSchema)
                .load("hdfs:///user/maperazzo/categories.tsv");

        return new Pair(trajectories, venues);
    }

    private static final void PopulateUsingTrajectories(List<Row> nodes, List<Row> edges, Dataset<Row> trajectories) {
        long prevUserId = -1L;
        long prevTrajId = -1L;
        for (Row t : trajectories.collectAsList()) {
            if (prevTrajId != -1L && prevUserId == t.getLong(PARSE_TRJ_USER_ID))
                edges.add(RowFactory.create(prevTrajId, t.getLong(PARSE_TRJ_ID), "trajStep"));

            nodes.add(RowFactory.create(t.getLong(PARSE_TRJ_ID), null, t.getLong(PARSE_TRJ_USER_ID), t.getDate(PARSE_TRJ_DATE),
                    t.getLong(PARSE_TRJ_TPOS), "Stop"));
            edges.add(RowFactory.create(t.getLong(PARSE_TRJ_ID), getVenueId(t.getString(PARSE_TRJ_VENUE_ID)), "isVenue"));

            prevUserId = t.getLong(PARSE_TRJ_USER_ID);
            prevTrajId = t.getLong(PARSE_TRJ_ID);
        }
    }

    private static final void PopulateUsingVenues(List<Row> nodes, List<Row> edges, Dataset<Row> venues) {

        for (Row v : venues.collectAsList()) {
            nodes.add(RowFactory.create(getVenueId(v.getString(PARSE_VNU_ID)), v.getString(PARSE_VNU_ID), null, null, null, "Venues"));
            nodes.add(RowFactory.create(getCategoryId(v.getString(PARSE_VNU_CATEGORY)), v.getString(PARSE_VNU_CATEGORY),
                    null, null, null, "Categories"));
            nodes.add(RowFactory.create(getCattypeId(v.getString(PARSE_VNU_CATTYPE)), v.getString(PARSE_VNU_CATTYPE),
                    null, null, null, "Category"));

            edges.add(RowFactory.create(getVenueId(v.getString(PARSE_VNU_ID)), getCategoryId(v.getString(PARSE_VNU_CATEGORY)),
                    "hasCategory"));
            edges.add(RowFactory.create(getCategoryId(v.getString(PARSE_VNU_CATEGORY)),
                    getCattypeId(v.getString(PARSE_VNU_CATTYPE)), "subCategoryOf"));
        }
    }

    // metadata
    private static StructType CreateVertexSchema()
    {
        List<StructField> vertFields = new ArrayList<StructField>();
        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertFields.add(DataTypes.createStructField("secondId", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("userId",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("utctimestamp",DataTypes.DateType, true));
        vertFields.add(DataTypes.createStructField("tpos",DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("label",DataTypes.StringType, false));

        return DataTypes.createStructType(vertFields);
    }

    // metadata
    private static StructType CreateEdgeSchema()
    {
        List<StructField> edgeFields = new ArrayList<StructField>();

        edgeFields.add(DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst", DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("label", DataTypes.StringType, false));

        return DataTypes.createStructType(edgeFields);
    }

    private static long getVenueId(String key) {
        if (venuesId.containsKey(key))
            return venuesId.get(key);
        else
            venuesId.put(key, venuesIdMax);
        return venuesIdMax++;
    }

    private static long getCategoryId(String key) {
        if (categoriesId.containsKey(key))
            return categoriesId.get(key);
        else
            categoriesId.put(key, categoriesIdMax);
        return categoriesIdMax++;
    }

    private static long getCattypeId(String key) {
        if (cattypeId.containsKey(key))
            return cattypeId.get(key);
        else
            cattypeId.put(key, cattypeIdMax);
        return cattypeIdMax++;
    }
}
