package com.csfrez.flink.cdc.function;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.BaseBean;
import com.csfrez.flink.cdc.bean.BinlogBean;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.factory.ProcessFactory;
import com.csfrez.flink.cdc.service.ProcessService;
import com.csfrez.flink.cdc.thread.StringThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BinlogFlatMapFunction implements FlatMapFunction<String, StatementBean> {

    private Map<String, Class<?>> clazzMap = new ConcurrentHashMap<>();
    private String active = "";

    public BinlogFlatMapFunction(String active){
        this.active = active;
    }

    @Override
    public void flatMap(String value, Collector<StatementBean> out) throws Exception {
        try {
            StringThreadLocal.set(active);

            BinlogBean binlogBean = BinlogBean.builder(value);
            BinlogBean.Source source = binlogBean.getSource();
            String name = source.getDb() + "." + source.getTable();
            JSONObject before = binlogBean.getBefore();
            JSONObject after = binlogBean.getAfter();
            System.out.println(name + ".before=" + binlogBean.getBefore());
            System.out.println(name + ".after=" + binlogBean.getAfter());

            if(before != null && after != null){
                this.collectStatementBean(name, OperationTypeEnum.UPDATE.value(), this.convertBean(name, after), out);
            } else if(before ==null && after != null){
                this.collectStatementBean(name, OperationTypeEnum.INSERT.value(), this.convertBean(name, after), out);
            } else if(before !=null && after == null){
                this.collectStatementBean(name, OperationTypeEnum.DELETE.value(), this.convertBean(name, before), out);
            } else {
                System.out.println(name + "====》无法确定的操作类型");
            }
        } catch (Exception e){
            e.printStackTrace();
            log.error("BinlogFlatMapFunction.flatMap", e);
        } finally {
            StringThreadLocal.remove();
        }
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
