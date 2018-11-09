package ar.edu.itba.nosql;

import java.sql.*;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hello world!
 */
public class App {
    public static void main(final String[] args) {

        final String url = "jdbc:postgresql://localhost:5432/postgres";
        final String user = "postgres";
        final String password = "root";

        try (Connection con = DriverManager.getConnection(url, user, password)){
            final Statement st = con.createStatement();
            final ResultSet users = st.executeQuery("SELECT DISTINCT userId from trajectories");

            while (users.next()) {
                final PreparedStatement s = con.prepareStatement("SELECT * from trajectories where userid = ? order by tpos");
                s.setString(1, users.getString(0));
                final ResultSet userTrajectory = s.executeQuery();
                final Queue<Trajectory>

                while (userTrajectory.next()) {

                }
            }
        } catch(SQLException ex){

            Logger lgr = Logger.getLogger(App.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
