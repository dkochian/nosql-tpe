package ar.edu.itba.nosql;

import org.apache.commons.math3.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public abstract class Population {

    public static Pair<Dataset<Row>, Dataset<Row>> LoadVenuesAndTrajectories(SQLContext sqlContext, String trajPath,
                                                                             String venuesPath) {

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

        Dataset<Row> trajectories = sqlContext.read().format("csv").option("delimiter","\t").option("header", "false")
                .schema(trajectorySchema)
                .load("hdfs:///user/maperazzo/" + trajPath);

        Dataset<Row> venues = sqlContext.read().format("csv").option("delimiter","\t").option("header", "false")
                .schema(venueSchema)
                .load("hdfs:///user/maperazzo/" + venuesPath);

        return new Pair(trajectories, venues);
    }
}
