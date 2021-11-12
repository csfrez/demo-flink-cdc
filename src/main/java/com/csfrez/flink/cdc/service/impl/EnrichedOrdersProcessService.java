package com.csfrez.flink.cdc.service.impl;

import com.csfrez.flink.cdc.bean.BaseBean;
import com.csfrez.flink.cdc.bean.OrderBean;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.service.ProcessService;

public class EnrichedOrdersProcessService implements ProcessService {

    @Override
    public StatementBean process(String name, String operationType, BaseBean baseBean) {
        if(OperationTypeEnum.INSERT.value().equals(operationType)){

        } else if(OperationTypeEnum.UPDATE.value().equals(operationType)){
            return this.getUpdateStatement(name, operationType, baseBean);
        } else if(OperationTypeEnum.DELETE.value().equals(operationType)){

        } else {

        }
        return null;
    }


    private StatementBean getUpdateStatement(String name, String operationType, BaseBean baseBean){
        StatementBean statementBean = new StatementBean();
        if(baseBean instanceof OrderBean) {
            OrderBean orderBean = (OrderBean) baseBean;
            statementBean.setOperationType(operationType);
            statementBean.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
            StringBuffer sb = new StringBuffer();
            sb.append("update enriched_orders set ");
            //sb.append("order_date = ").append(orderBean.getOrder_date()).append(", ");
            sb.append("customer_name = '").append(orderBean.getCustomer_name()).append("' ");
            sb.append("where order_id=").append(orderBean.getOrder_id());
            statementBean.setSql(sb.toString());
            return statementBean;
        }
        return null;
    }
}
