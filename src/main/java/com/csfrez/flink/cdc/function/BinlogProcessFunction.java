package com.csfrez.flink.cdc.function;

import com.alibaba.fastjson2.JSONObject;
import com.csfrez.flink.cdc.bean.BinlogBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email csfrez@163.com
 */
public class BinlogProcessFunction extends ProcessFunction<String, BinlogBean> {

    @Override
    public void processElement(String value, Context ctx, Collector<BinlogBean> out) throws Exception {
        //System.out.println(value);
        BinlogBean binlogBean = JSONObject.parseObject(value, BinlogBean.class);
        BinlogBean.Source source = binlogBean.getSource();
        String name = source.getDb() + "." + source.getTable();
        ctx.output(new OutputTag<BinlogBean>(name){}, binlogBean);
        //out.collect(binlogBean);
        /*
        BinlogBean.Source source = binlogBean.getSource();
        String name = source.getDb() + "." + source.getTable();
        ProcessService processService = ProcessFactory.createProcess(name);
        processService.process(binlogBean.getBefore(), binlogBean.getAfter(), source);
        */
    }
}
