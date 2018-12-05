package ar.edu.itba.nosql;

import ar.edu.itba.nosql.utils.Converter;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.neo4j.driver.v1.*;
import org.neo4j.spark.Neo4j;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.*;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jPopulation extends Population implements AutoCloseable {

    private static final Driver driver = GraphDatabase.driver("bolt://node1.it.itba.edu.ar:7689", AuthTokens.basic("jdantur", "jdantur") );

    private static final int PARSE_TRJ_ID = 0;
    private static final int PARSE_TRJ_USER_ID = 1;
    private static final int PARSE_TRJ_VENUE_ID = 2;
    private static final int PARSE_TRJ_DATE = 3;
    private static final int PARSE_TRJ_TPOS = 4;

    private static final int PARSE_VNU_ID = 0;
    private static final int PARSE_VNU_CATEGORY = 1;
    private static final int PARSE_VNU_CATTYPE = 4;

    private static final Set<String> categories = new HashSet<>();
    private static final Set<String> cattypes = new HashSet<>();

    private static Converter converter = new Converter();

    private static final String TRAJECTORIES_FILE_NAME_INPUT = "prunned_trajectoriesss.tsv";
    private static final String VENUES_FILE_NAME_INPUT = "categories.tsv";


    public static void main (String[] args) {

        SparkSession sp = SparkSession.builder().appName("Neo4j Population")
                .config("spark.neo4j.bolt.url","bolt://node1.it.itba.edu.ar:7689")
                .config("spark.neo4j.bolt.user","jdantur")
                .config("spark.neo4j.bolt.password","jdantur")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(sp);

        try ( Session session = driver.session() ) {
            //empty database
            session.writeTransaction((tx -> tx.run("MATCH (n) DETACH DELETE n;")));

            Pair<Dataset<Row>, Dataset<Row>> files = LoadVenuesAndTrajectories(sqlContext, TRAJECTORIES_FILE_NAME_INPUT,
                    VENUES_FILE_NAME_INPUT);

            PopulateUsingVenues(session, files.getValue());
            PopulateUsingTrajectories(session, files.getKey());

        }
    }

    private static final void PopulateUsingVenues(final Session session, final Dataset<Row> venues) {

        for (Row v : venues.collectAsList()) {

            session.writeTransaction(tx -> tx.run("CREATE (n:Venue { id: $venueId })",
                    parameters("venueId", v.getString(PARSE_VNU_ID))));

            if (!categories.contains(v.getString(PARSE_VNU_CATEGORY))) {
                categories.add(v.getString(PARSE_VNU_CATEGORY));
                session.writeTransaction(tx -> tx.run("CREATE (n:Categories { name: $category })",
                        parameters("category", v.getString(PARSE_VNU_CATEGORY))));

                if (!cattypes.contains(v.getString(PARSE_VNU_CATTYPE))) {
                    cattypes.add(v.getString(PARSE_VNU_CATTYPE));
                    session.writeTransaction(tx -> tx.run("CREATE (n:Category { name: $cattype })",
                            parameters("cattype", v.getString(PARSE_VNU_CATTYPE))));
                }

                session.writeTransaction(tx -> tx.run("MATCH (c1:Categories),(c2:Category)\n" +
                        "WHERE c1.name = $category and c2.name = $cattype\n" +
                        "CREATE (c1)-[r:subCategoryOf]->(c2)\n",
                        parameters("category", v.getString(PARSE_VNU_CATEGORY),
                                "cattype", v.getString(PARSE_VNU_CATTYPE))));
            }

            session.writeTransaction(tx -> tx.run("MATCH (v:Venue),(c:Categories)\n" +
                    "WHERE v.id = $venueId and c.name = $category\n" +
                    "CREATE (v)-[r:hasCategory]->(c)\n",
                    parameters("venueId", v.getString(PARSE_VNU_ID),
                            "category", v.getString(PARSE_VNU_CATEGORY))));
        }
    }

    private static final void PopulateUsingTrajectories(final Session session, final Dataset<Row> trajectories) {
        long prevUserId = -1L;
        long prevTrajId = -1L;
        for (Row t : trajectories.collectAsList()) {

            session.writeTransaction(tx -> tx.run("CREATE (n:Stop { id: $trjId, userId: $userId, utctimestamp: $date, tpos: $tpos })",
                    parameters("trjId", t.getLong(PARSE_TRJ_ID),
                            "userId", t.getLong(PARSE_TRJ_USER_ID),
                            "date", t.getDate(PARSE_TRJ_DATE).toString(),
                            "tpos", t.getLong(PARSE_TRJ_TPOS))));

            session.writeTransaction(tx -> tx.run("MATCH (s:Stop),(v:Venue)\n" +
                    "WHERE s.id = $trjId and v.id = $venueId\n" +
                    "CREATE (s)-[r:isVenue]->(v)\n",
                    parameters("trjId", t.getLong(PARSE_TRJ_ID),
                            "venueId", t.getString(PARSE_TRJ_VENUE_ID))));

            if (prevUserId == t.getLong(PARSE_TRJ_USER_ID))
                WriteTrajStep(session, t, prevTrajId);


            prevUserId = t.getLong(PARSE_TRJ_USER_ID);
            prevTrajId = t.getLong(PARSE_TRJ_ID);

        }
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public final static void WriteTrajStep(Session s, Row t, final long prevTrajId) {
        s.writeTransaction((tx -> tx.run("MATCH (s1:Stop),(s2:Stop)\n" +
                        "WHERE s1.id = $prevTrjId and s2.id = $trjId\n" +
                        "CREATE (s1)-[r:trajStep]->(s2)\n",
                parameters("prevTrjId", prevTrajId,
                        "trjId", t.getLong(PARSE_TRJ_ID)))));
    }
}
