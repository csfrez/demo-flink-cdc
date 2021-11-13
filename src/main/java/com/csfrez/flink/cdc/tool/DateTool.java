package com.csfrez.flink.cdc.tool;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTool {

    private static final String strDateFormat = "yyyy-MM-dd HH:mm:ss";

    public static String dateToString(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
        return sdf.format(date);
    }
}
