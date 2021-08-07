package org.example.util;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

public class TimeUtils {

    private static final FastDateFormat instance;

    static {
        instance = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    }

    public static long getTimestampFromDateTime(String dateTime){
        try {
            return instance.parse(dateTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return -1;
        }

    }

    public static String formatDateTimeFromTimestamp(long timestamp){
        return instance.format(timestamp);
    }
}
