package com.csfrez.flink.cdc.service.impl;

import com.csfrez.flink.cdc.bean.*;
import com.csfrez.flink.cdc.dao.ProductDao;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.tool.DateTool;
import com.csfrez.flink.cdc.tool.ParamTool;
import com.csfrez.flink.cdc.tool.StringTool;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.service.ProcessService;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.sql.JDBCType.NULL;

public class EnrichedOrdersProcessService implements ProcessService {

    @Override
    public StatementBean process(String name, String operationType, BaseBean baseBean) {
        if(OperationTypeEnum.INSERT.value().equals(operationType)){
            return this.getInsertPrepareStatement(name, operationType, baseBean);
        } else if(OperationTypeEnum.UPDATE.value().equals(operationType)){
            return this.getUpdatePrepareStatement(name, operationType, baseBean);
        } else if(OperationTypeEnum.DELETE.value().equals(operationType)){
            return this.getDeleteStatement(name, operationType, baseBean);
        } else {
            return null;
        }
    }

    private StatementBean getInsertPrepareStatement(String name, String operationType, BaseBean baseBean){
        PrepareStatementBean prepareStatement = new PrepareStatementBean();
        if(baseBean instanceof OrderBean) {
            prepareStatement.setOperationType(operationType);
            prepareStatement.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
            OrderBean orderBean = (OrderBean) baseBean;
            String sql = "insert into `enriched_orders` (`order_id`, `order_date`, `customer_name`, `price`, `product_id`, `order_status`, `name`, `description`, `shipment_id`, `origin`, `destination`, `is_arrived`) " +
                    "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            prepareStatement.setSql(sql);
            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(orderBean.getOrder_id(), 1, Long.class));
            paramList.add(ParamTool.getParam(orderBean.getOrder_date(), 2, Date.class));
            paramList.add(ParamTool.getParam(orderBean.getCustomer_name(), 3, String.class));
            paramList.add(ParamTool.getParam(orderBean.getPrice(), 4, BigDecimal.class));
            paramList.add(ParamTool.getParam(orderBean.getProduct_id(), 5, Long.class));
            paramList.add(ParamTool.getParam(orderBean.getOrder_status(), 6, Integer.class));
            ProductBean productBean = ProductDao.getProductBeanById(orderBean.getProduct_id());
            if(productBean != null){
                paramList.add(ParamTool.getParam(productBean.getName(), 7, String.class));
                paramList.add(ParamTool.getParam(productBean.getDescription(), 8, String.class));
            } else {
                paramList.add(ParamTool.getParam(null, 7, String.class));
                paramList.add(ParamTool.getParam(null, 8, String.class));
            }
            paramList.add(ParamTool.getParam(null, 9, String.class));
            paramList.add(ParamTool.getParam(null, 10, String.class));
            paramList.add(ParamTool.getParam(null, 11, String.class));
            paramList.add(ParamTool.getParam(null, 12, String.class));
            prepareStatement.setParamList(paramList);
            return prepareStatement;
        } else if(baseBean instanceof ProductBean){
            return this.getUpdatePrepareStatement(name, operationType, baseBean);
        } else if(baseBean instanceof ShipmentBean){
            return this.getUpdatePrepareStatement(name, operationType, baseBean);
        }
        return null;
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

    private StatementBean getUpdatePrepareStatement(String name, String operationType, BaseBean baseBean) {
        PrepareStatementBean prepareStatement = new PrepareStatementBean();
        prepareStatement.setOperationType(operationType);
        prepareStatement.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
        if(baseBean instanceof OrderBean) {
            OrderBean orderBean = (OrderBean) baseBean;
            String sql = "update enriched_orders set order_date = ?, customer_name = ?, price = ?, product_id = ?, order_status = ? where order_id = ?";
            prepareStatement.setSql(sql);

            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(orderBean.getOrder_date(), 1, Date.class));
            paramList.add(ParamTool.getParam(orderBean.getCustomer_name(), 2, String.class));
            paramList.add(ParamTool.getParam(orderBean.getPrice(), 3, BigDecimal.class));
            paramList.add(ParamTool.getParam(orderBean.getProduct_id(), 4, Long.class));
            paramList.add(ParamTool.getParam(orderBean.getOrder_status(), 5, Integer.class));
            paramList.add(ParamTool.getParam(orderBean.getOrder_id(), 6, Long.class));
            prepareStatement.setParamList(paramList);

            return prepareStatement;
        } else if(baseBean instanceof ProductBean){
            ProductBean productBean = (ProductBean)baseBean;
            String sql = "update enriched_orders set name = ?, description = ? where product_id = ?";
            prepareStatement.setSql(sql);

            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(productBean.getName(), 1, String.class));
            paramList.add(ParamTool.getParam(productBean.getDescription(), 2, String.class));
            paramList.add(ParamTool.getParam(productBean.getId(), 3, Long.class));
            prepareStatement.setParamList(paramList);

            return prepareStatement;
        } else if(baseBean instanceof ShipmentBean){
            ShipmentBean shipmentBean = (ShipmentBean)baseBean;
            String sql = "update enriched_orders set shipment_id = ?, origin = ?, destination = ?, is_arrived = ? where order_id = ?";
            prepareStatement.setSql(sql);

            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(shipmentBean.getShipment_id(), 1, Long.class));
            paramList.add(ParamTool.getParam(shipmentBean.getOrigin(), 2, String.class));
            paramList.add(ParamTool.getParam(shipmentBean.getDestination(), 3, String.class));
            paramList.add(ParamTool.getParam(shipmentBean.getIs_arrived(), 4, Integer.class));
            paramList.add(ParamTool.getParam(shipmentBean.getOrder_id(), 5, Long.class));
            prepareStatement.setParamList(paramList);

            return prepareStatement;
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
