package com.csfrez.flink.cdc.bean;


import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email csfrez@163.com
 */
@Data
public class BinlogBean {

    private JSONObject before;

    private JSONObject after;

    private Source source;

    private String op;

    private Long ts_ms;

    private String transaction;

    @Data
    public static class Source {

        private String version;

        private String connector;

        private String name;

        private String db;

        private String table;

        private Long ts_ms;

    }

    public static BinlogBean builder(String json){
        return JSONObject.parseObject(json, BinlogBean.class);
    }

}


