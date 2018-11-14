package ar.edu.itba.nosql;

import ar.edu.itba.nosql.entities.Configuration;
import ar.edu.itba.nosql.entities.Trajectory;
import ar.edu.itba.nosql.entities.Venue;
import ar.edu.itba.nosql.utils.IOManager;
import ar.edu.itba.nosql.utils.OutputWriter;
import ar.edu.itba.nosql.utils.Point;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class Pruning {

    private static final Logger logger = LoggerFactory.getLogger(Pruning.class);

    public static void main(String[] args) {
        final Configuration configuration = IOManager.getConfiguration();

        final double velocity = configuration.getVelocity();

        final Queue<Trajectory> q = new ArrayDeque<>();

        final OutputWriter outputWriter = new OutputWriter();
        final List<ResultSet> userList = new ArrayList<>();
        outputWriter.remove();

        try (Connection con = DriverManager.getConnection(configuration.getUrl(), configuration.getUser(),
                configuration.getPassword())) {

            final PreparedStatement st = con.prepareStatement("SELECT DISTINCT userId from ?");
            st.setString(1, configuration.getName());

            final ResultSet userRS = st.executeQuery();

            while (userRS.next())
                userList.add(userRS);

            userList.forEach(users -> {
                try {
                    final PreparedStatement s;
                    s = con.prepareStatement("SELECT * from ? where userid = ? order by tpos");
                    s.setString(1, configuration.getName());
                    s.setInt(2, users.getInt(1));
                    final ResultSet userTrajectory = s.executeQuery();

                    while (userTrajectory.next()) {
                        final PreparedStatement s2 = con.prepareStatement("SELECT * from categories where venueid = ?");
                        s2.setString(1, userTrajectory.getString(2));
                        final ResultSet venue = s2.executeQuery();

                        while (venue.next()) {
                            Venue auxVenue = new Venue(venue.getString(1), new Point<>(venue.getDouble(3),
                                    venue.getDouble(4)), venue.getString(2), venue.getString(5));
                            q.add(new Trajectory(userTrajectory.getLong(1), userTrajectory.getInt(2), auxVenue, new DateTime(userTrajectory.getDate(4)), userTrajectory.getInt(5)));

                        }
                    }

                    int tpos = 1;
                    final Queue<Trajectory> userTrajectoryPruned = new ArrayDeque<>();
                    Trajectory previous = q.poll();
                    userTrajectoryPruned.add(previous);

                    while (!q.isEmpty()) {
                        final Trajectory current = q.poll();

                        if (!previous.getVenue().equals(current.getVenue()) && velocity > current.getVelocity(previous)) {
                            current.setTpos(++tpos);
                            userTrajectoryPruned.add(current);
                            previous = current;
                        }
                    }
                    try {
                        outputWriter.write(userTrajectoryPruned);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }
}
