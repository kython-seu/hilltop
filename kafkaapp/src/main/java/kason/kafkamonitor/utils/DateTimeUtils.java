package kason.kafkamonitor.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhangkai12 on 2017/12/25.
 */
public class DateTimeUtils {
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public static String getTimeFormat(String time){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(FORMAT);

        return simpleDateFormat.format(new Date(Long.parseLong(time)));
    }
}
