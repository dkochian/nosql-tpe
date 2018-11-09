package ar.edu.itba.nosql;

import ar.edu.itba.nosql.entities.Trajectory;

import java.sql.*;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) {

        //final String url = "jdbc:postgresql://node3.it.itba.edu.ar:5453/grupo1";
        final String url = "jdbc:postgresql://127.0.0.1:5453/grupo1";
        final String user = "grupo1";
        final String password = "grupo1";

        final double testingVelocity = 1.1;

        try (Connection con = DriverManager.getConnection(url, user, password)) {
            final Statement st = con.createStatement();
            final ResultSet users = st.executeQuery("SELECT DISTINCT userId from trajectories");

            while (users.next()) {
                final PreparedStatement s = con.prepareStatement("SELECT * from trajectories where userid = ? order by tpos");
                s.setInt(1, users.getInt(1));
                final ResultSet userTrajectory = s.executeQuery();
                final Queue<Trajectory> q = new ArrayDeque<>();

                while (userTrajectory.next()) {
                    final PreparedStatement s2 = con.prepareStatement("SELECT * from categories where venueid = ?");
                    s2.setString(1, userTrajectory.getString(2));
                    final ResultSet venue = s2.executeQuery();
                    System.out.println(venue.getDouble(3) + " - " + venue.getDouble(4) + " - " + venue.getString(2));
                    //Trajectory t = new Trajectory()
                }
            }
        } catch (SQLException ex) {

            Logger lgr = Logger.getLogger(App.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
