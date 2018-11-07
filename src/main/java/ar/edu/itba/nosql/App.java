package ar.edu.itba.nosql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "root";

        try (Connection con = DriverManager.getConnection(url, user, password);
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SELECT * from trajectories where userid=0 order by tpos")) {

            while (rs.next()) {
                System.out.println(rs.getString(1)+ "|" + rs.getString(2)+"|"+rs.getString(3));
            }

        } catch (SQLException ex) {

            Logger lgr = Logger.getLogger(App.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
