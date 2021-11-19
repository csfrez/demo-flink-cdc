package com.csfrez.flink.cdc.sink;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.BinlogBean;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;

public class StringSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
        try{
            BinlogBean binlogBean = JSONObject.parseObject(value, BinlogBean.class);
            JSONObject after = binlogBean.getAfter();
            BigDecimal price = after.getBigDecimal("price");
            System.out.println("price="+price);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
