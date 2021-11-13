package com.csfrez.flink.cdc.tool;

public class StringTool {

    public static String replaceQuota(String str){
        if(str != null){
            str.replace("\'", "\\'");
        }
        return str;
    }
}
