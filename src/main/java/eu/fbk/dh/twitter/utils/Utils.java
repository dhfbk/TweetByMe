package eu.fbk.dh.twitter.utils;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class Utils {

    public static TimeZone DEFAULT_TIMEZONE = TimeZone.getTimeZone(ZoneId.of("UTC"));

    public static String unixToTime(long input_time) {
        return unixToTime(input_time, DEFAULT_TIMEZONE);
    }

    public static String unixToTime(long input_time, TimeZone timeZone) {
        Date time = new Date(input_time * 1000);
        final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
        final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
        sdf.setTimeZone(timeZone);
        return sdf.format(time) + "Z";
    }

    public static Long timeToUnix(int year, int month, int day, int hour, int minute, int second) {
        return timeToUnix(year, month, day, hour, minute, second, DEFAULT_TIMEZONE);
    }

    public static Long timeToUnix(int year, int month, int day, int hour, int minute, int second, TimeZone timeZone) {
        Calendar time = Calendar.getInstance(timeZone);
        time.set(year, month - 1, day, hour, minute, second);
        return time.getTimeInMillis() / 1000L;
    }
}
