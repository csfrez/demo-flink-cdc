package com.csfrez.flink.cdc.service.impl;

import com.csfrez.flink.cdc.bean.BaseBean;
import com.csfrez.flink.cdc.bean.OrderBean;
import com.csfrez.flink.cdc.bean.ProductBean;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.dao.ProductDao;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.service.ProcessService;
import com.csfrez.flink.cdc.tool.DateTool;
import com.csfrez.flink.cdc.tool.StringTool;

import static java.sql.JDBCType.NULL;

public class EnrichedOrdersProcessService implements ProcessService {

    @Override
    public StatementBean process(String name, String operationType, BaseBean baseBean) {
        if(OperationTypeEnum.INSERT.value().equals(operationType)){
            return this.getInsertStatement(name, operationType, baseBean);
        } else if(OperationTypeEnum.UPDATE.value().equals(operationType)){
            return this.getUpdateStatement(name, operationType, baseBean);
        } else if(OperationTypeEnum.DELETE.value().equals(operationType)){
            return this.getDeleteStatement(name, operationType, baseBean);
        } else {
            return null;
        }
    }

    private StatementBean getInsertStatement(String name, String operationType, BaseBean baseBean){
        StatementBean statementBean = new StatementBean();
        if(baseBean instanceof OrderBean) {
            statementBean.setOperationType(operationType);
            statementBean.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
            OrderBean orderBean = (OrderBean) baseBean;
            StringBuffer sb = new StringBuffer();
            sb.append("insert into `enriched_orders` (`order_id`, `order_date`, `customer_name`, `price`, `product_id`, `order_status`, `name`, `description`, `shipment_id`, `origin`, `destination`, `is_arrived`) values( ");
            sb.append("'").append(orderBean.getOrder_id()).append("', ");
            sb.append("'").append(DateTool.dateToString(orderBean.getOrder_date())).append("', ");
            sb.append("'").append(orderBean.getCustomer_name()).append("', ");
            sb.append("'").append(orderBean.getPrice()).append("', ");
            sb.append("'").append(orderBean.getProduct_id()).append("', ");
            sb.append("'").append(orderBean.getOrder_status()).append("', ");

            ProductBean productBean = ProductDao.getProductBeanById(orderBean.getProduct_id());
            if(productBean != null){
                sb.append("'").append(productBean.getName()).append("', ");
                sb.append("'").append(StringTool.replaceQuota(productBean.getDescription())).append("', ");
            } else {
                sb.append(NULL).append(", ").append(NULL).append(", ");
            }
            sb.append(NULL).append(", ").append(NULL).append(", ").append(NULL).append(", ").append(NULL).append(")");
            statementBean.setSql(sb.toString());
            return statementBean;
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
        } else if(baseBean instanceof ProductBean){
            ProductBean productBean = (ProductBean)baseBean;
            statementBean.setOperationType(operationType);
            statementBean.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
            StringBuffer sb = new StringBuffer();
            sb.append("update enriched_orders set ");
            sb.append("name = '").append(productBean.getName()).append("', ");
            sb.append("description = '").append(productBean.getDescription()).append("' ");
            sb.append("where product_id=").append(productBean.getId());
            statementBean.setSql(sb.toString());
            return statementBean;
        }
        return null;
    }

    private StatementBean getDeleteStatement(String name, String operationType, BaseBean baseBean){
        StatementBean statementBean = new StatementBean();
        if(baseBean instanceof OrderBean) {
            OrderBean orderBean = (OrderBean) baseBean;
            String sql = "delete from enriched_orders where order_id = " + orderBean.getOrder_id();
            statementBean.setOperationType(operationType);
            statementBean.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
            statementBean.setSql(sql);
            return statementBean;
        }
        return null;
    }
}
