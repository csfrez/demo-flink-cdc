package com.csfrez.flink.cdc.function;

import com.csfrez.flink.cdc.bean.BinlogBean;
import com.csfrez.flink.cdc.config.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Map;

@Slf4j
public class BinlogFilterFunction implements FilterFunction<String> {

    @Override
    public boolean filter(String value) throws Exception {
        try{
            //log.info(value);
            BinlogBean binlogBean = BinlogBean.builder(value);
            BinlogBean.Source source = binlogBean.getSource();
            String name = source.getDb() + "." + source.getTable();
            Map<String, TableConfig> tableConfig = TableConfig.getTableConfig();
            if(tableConfig.containsKey(name) && "src".equals(tableConfig.get(name).getTableType())){
                return true;
            }
            log.info("BinlogFilterFunction.filter={}", name + "不是配置的数据源表，不处理");
        } catch (Exception e){
            e.printStackTrace();
            log.error("BinlogFilterFunction.filter", e);
        }
        return false;
    }
}
