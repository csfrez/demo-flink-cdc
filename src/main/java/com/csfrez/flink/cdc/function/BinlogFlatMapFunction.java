package com.csfrez.flink.cdc.function;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.*;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.factory.ProcessFactory;
import com.csfrez.flink.cdc.service.ProcessService;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BinlogFlatMapFunction implements FlatMapFunction<String, StatementBean> {

    private Map<String, Class<?>> clazzMap = new ConcurrentHashMap<>();

    public BinlogFlatMapFunction(){

    }


    @Override
    public void flatMap(String value, Collector<StatementBean> out) throws Exception {
        BinlogBean binlogBean = BinlogBean.builder(value);
        BinlogBean.Source source = binlogBean.getSource();
        String name = source.getDb() + "." + source.getTable();

        JSONObject before = binlogBean.getBefore();
        JSONObject after = binlogBean.getAfter();
        System.out.println(name + ".before=" + binlogBean.getBefore());
        System.out.println(name + ".after=" + binlogBean.getAfter());

        if(before != null && after != null){
            this.collectStatementBean(name, OperationTypeEnum.UPDATE.value(), this.convertBean(name, after), out);
        }
//        Class<?> aClass = Class.forName(tableConfig.getBeanReference());
//        Object o = binlogBean.getBefore().toJavaObject(Class.forName(tableConfig.getBeanReference()));
//        OrderBean orderBeanBefore = binlogBean.getBefore().toJavaObject(OrderBean.class);
//        OrderBean orderBeanAfter = binlogBean.getAfter().toJavaObject(OrderBean.class);
//        out.collect(orderBeanAfter);
    }

    private BaseBean convertBean(String name, JSONObject jsonObject) throws ClassNotFoundException {
        TableConfig tableConfig = TableConfig.getTableConfig(name);
        Class<?> clazz = Class.forName(tableConfig.getBeanReference());
        return (BaseBean) jsonObject.toJavaObject(clazz);
    }

    private void collectStatementBean(String name, String operationType, BaseBean baseBean, Collector<StatementBean> out){
        String[] relateTables = TableConfig.getTableConfig(name).getRelateTables().split(",");
        for(String tableName : relateTables){
            ProcessService processService = ProcessFactory.createProcess(tableName);
            if(processService == null){
                continue;
            }
            StatementBean statementBean = processService.process(tableName, operationType, baseBean);
            if(statementBean == null){
                continue;
            }
            out.collect(statementBean);
        }
    }
}
