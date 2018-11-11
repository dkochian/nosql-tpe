package ar.edu.itba.nosql.entities;

import ar.edu.itba.nosql.utils.Point;

import java.util.Objects;

public class Venue {

    private final String id;

    private final Point<Double> position;

    private final String category;

    private final String categoryType;

    public Venue(String id, Point<Double> position, String category, String categoryType) {
        this.id = id;
        this.position = position;
        this.category = category;
        this.categoryType = categoryType;
    }

    public String getId() {
        return id;
    }

    public Point<Double> getPosition() {
        return position;
    }

    public String getCategory() {
        return category;
    }

    public String getCategoryType() {
        return categoryType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Venue venue = (Venue) o;
        return Objects.equals(id, venue.id);
    }

    @Override
    public String toString() {
        return "Venue{" +
                "id='" + id + '\'' +
                ", position=" + position +
                ", category='" + category + '\'' +
                ", categoryType='" + categoryType + '\'' +
                '}';
    }

    public double getDistance(final Venue venue) {
        return distance(position.getlatitude(), venue.getPosition().getlatitude(), position.getlongitude(),
                venue.getPosition().getlongitude(), 0D, 0D);
    }

    //https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude-what-am-i-doi
    /**
     * Calculate distance between two points in latitude and longitude taking
     * into account height difference. If you are not interested in height
     * difference pass 0.0. Uses Haversine method as its base.
     *
     * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
     * el2 End altitude in meters
     * @returns Distance in Meters
     */
    private static double distance(double lat1, double lat2, double lon1,
                                  double lon2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c;

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return Math.sqrt(distance);
    }
}
