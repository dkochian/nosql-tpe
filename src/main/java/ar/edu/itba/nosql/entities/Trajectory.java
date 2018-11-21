package ar.edu.itba.nosql.entities;

import org.joda.time.DateTime;


public class Trajectory {

    private final long id;

    private final int userId;

    private final Venue venue;

    private final DateTime date;

    private long tpos;

    public Trajectory(long id, int userId, Venue venue, DateTime date, long tpos) {
        this.id = id;
        this.userId = userId;
        this.venue = venue;
        this.date = date;
        this.tpos = tpos;
    }

    public long getId() {
        return id;
    }

    public int getUserId() {
        return userId;
    }

    public Venue getVenue() {
        return venue;
    }

    public DateTime getDate() {
        return date;
    }

    public long getTpos() {
        return tpos;
    }

    public void setTpos(int tpos) {
        this.tpos = tpos;
    }

    //hours
    public double getTimeDifference(final Trajectory t) {
        return (date.getMillis() - t.getDate().getMillis()) / (1000 * 60 * 60);
    }

    public double getVelocity(final Trajectory t) {
        return venue.getDistance(t.getVenue())/getTimeDifference(t);
    }

    @Override
    public String toString() {
        return "Trajectory{" +
                "userId=" + userId +
                ", venue=" + venue +
                ", date=" + date +
                ", tpos=" + tpos +
                '}';
    }
}
