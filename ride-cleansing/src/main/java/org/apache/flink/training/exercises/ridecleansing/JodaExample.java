package org.apache.flink.training.exercises.ridecleansing;
import org.joda.time.Interval;
import org.joda.time.DateTime;

public class JodaExample {
    public static void main(String[] args) {
        DateTime start = new DateTime(2023, 1, 1, 0, 0);
        DateTime end = new DateTime(2023, 12, 31, 23, 59);
        Interval interval = new Interval(start, end);
        System.out.println("Interval: " + interval);
    }
}
