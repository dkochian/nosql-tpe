package ar.edu.itba.nosql.entities;

import ar.edu.itba.nosql.utils.Point;

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
    public String toString() {
        return "Venue{" +
                "id='" + id + '\'' +
                ", position=" + position +
                ", category='" + category + '\'' +
                ", categoryType='" + categoryType + '\'' +
                '}';
    }
}
