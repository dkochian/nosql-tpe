package ar.edu.itba.nosql.entities;

public class Configuration {

    private final String tableName;

    private final int userAmount;

    private final int visitedVenues;

    private final double velocity;

    private final String url;

    private final String user;

    private final String password;

    public Configuration() {
        tableName = "trajectoriesss";
        userAmount = 1000;
        visitedVenues = 100;
        velocity = 36.0;

        url = "jdbc:postgresql://127.0.0.1:5453/grupo1"; // "jdbc:postgresql://node3.it.itba.edu.ar:5453/grupo1";
        user = "grupo1";
        password = "grupo1";
    }

    public Configuration(String tableName, int userAmount, int visitedVenues, double velocity, String url, String user, String password) {
        this.tableName = tableName;
        this.userAmount = userAmount;
        this.visitedVenues = visitedVenues;
        this.velocity = velocity;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public int getUserAmount() {
        return userAmount;
    }

    public int getVisitedVenues() {
        return visitedVenues;
    }

    public double getVelocity() {
        return velocity;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
