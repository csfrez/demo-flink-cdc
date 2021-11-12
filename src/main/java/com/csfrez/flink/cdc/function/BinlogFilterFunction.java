package com.csfrez.flink.cdc.function;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.BinlogBean;
import com.csfrez.flink.cdc.config.TableConfig;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Map;

public class BinlogFilterFunction implements FilterFunction<String> {

    @Override
    public boolean filter(String value) throws Exception {
        System.out.println(Thread.currentThread().getName() + value);
        BinlogBean binlogBean = BinlogBean.builder(value);
        BinlogBean.Source source = binlogBean.getSource();
        String name = source.getDb() + "." + source.getTable();
        Map<String, TableConfig> tableConfig = TableConfig.getTableConfig();
        if(tableConfig.containsKey(name)){
            return true;
        }
        System.out.println(name+"不是配置的数据表，不处理");
        return false;
    }
}
