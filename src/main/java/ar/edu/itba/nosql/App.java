package ar.edu.itba.nosql;

import ar.edu.itba.nosql.entities.Trajectory;
import ar.edu.itba.nosql.entities.Venue;
import ar.edu.itba.nosql.utils.OutputWriter;
import ar.edu.itba.nosql.utils.Point;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Level;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        //final String url = "jdbc:postgresql://node3.it.itba.edu.ar:5453/grupo1";
        final String url = "jdbc:postgresql://127.0.0.1:5453/grupo1";
        final String user = "grupo1";
        final String password = "grupo1";

        final double testingVelocity = 1.1;
        final double velocity = testingVelocity;

        final Queue<Trajectory> q = new ArrayDeque<>();

        final OutputWriter outputWriter = new OutputWriter();
        outputWriter.remove();

        try (Connection con = DriverManager.getConnection(url, user, password)) {
            final Statement st = con.createStatement();
            final ResultSet users = st.executeQuery("SELECT DISTINCT userId from trajectories");

            while (users.next()) {
                final PreparedStatement s = con.prepareStatement("SELECT * from trajectories where userid = ? order by tpos");
                s.setInt(1, users.getInt(1));
                final ResultSet userTrajectory = s.executeQuery();

                while (userTrajectory.next()) {
                    final PreparedStatement s2 = con.prepareStatement("SELECT * from categories where venueid = ?");
                    s2.setString(1, userTrajectory.getString(2));
                    final ResultSet venue = s2.executeQuery();

                    while(venue.next()){
                        Venue auxVenue = new Venue(venue.getString(1), new Point<>(venue.getDouble(3),
                                venue.getDouble(4)), venue.getString(2), venue.getString(5));
                        q.add(new Trajectory(userTrajectory.getInt(1), auxVenue, new DateTime(userTrajectory.getDate(3)), userTrajectory.getInt(4)));

                    }
                }

                final Queue<Trajectory> userTrajectoryPrunned = new ArrayDeque<>();

                int tpos = 1;
                Trajectory previous = q.poll();
                userTrajectoryPrunned.add(previous);
                while (!q.isEmpty()) {
                    Trajectory current = q.poll();

                    if (!previous.equals(current) && velocity > current.getVelocity(previous)) {
                        current.setTpos(++tpos);
                        userTrajectoryPrunned.add(current);
                        previous = current;
                    }
                }
                try {
                    outputWriter.write(userTrajectoryPrunned);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }
}
