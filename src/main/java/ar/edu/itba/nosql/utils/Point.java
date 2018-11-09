package ar.edu.itba.nosql.utils;

public class Point<T> {

    private final T longitude;
    private final T latitude;

    public Point(T longitude, T latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public T getlongitude() {
        return longitude;
    }

    public T getlatitude() {
        return latitude;
    }

    @Override
    public String toString() {
        return "Point{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }
}