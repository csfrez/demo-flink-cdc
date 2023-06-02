package com.csfrez.flink.cdc.service.impl;

import com.csfrez.flink.cdc.bean.*;
import com.csfrez.flink.cdc.config.TableConfig;
import com.csfrez.flink.cdc.dao.ProductDao;
import com.csfrez.flink.cdc.enumeration.OperationTypeEnum;
import com.csfrez.flink.cdc.service.ProcessService;
import com.csfrez.flink.cdc.tool.ParamTool;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
            String sql = "insert into `enriched_orders` (`order_id`, `order_date`, `customer_name`, `price`, `product_id`, `order_status`, `name`, `description`, `shipment_id`, `origin`, `destination`, `is_arrived`, `create_time`, `update_time`) " +
                    "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            prepareStatement.setSql(sql);
            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(1, orderBean.getOrder_id(), Long.class));
            paramList.add(ParamTool.getParam(2, orderBean.getOrder_date(), Date.class));
            paramList.add(ParamTool.getParam(3, orderBean.getCustomer_name(), String.class));
            paramList.add(ParamTool.getParam(4, orderBean.getPrice(), BigDecimal.class));
            paramList.add(ParamTool.getParam(5, orderBean.getProduct_id(), Long.class));
            paramList.add(ParamTool.getParam(6, orderBean.getOrder_status(), Integer.class));
            ProductBean productBean = ProductDao.getProductBeanById(orderBean.getProduct_id());
            if(productBean != null){
                paramList.add(ParamTool.getParam(7, productBean.getName(), String.class));
                paramList.add(ParamTool.getParam(8, productBean.getDescription(), String.class));
            } else {
                paramList.add(ParamTool.getParam(7, null, String.class));
                paramList.add(ParamTool.getParam(8, null, String.class));
            }
            paramList.add(ParamTool.getParam(9, null, String.class));
            paramList.add(ParamTool.getParam(10, null, String.class));
            paramList.add(ParamTool.getParam(11, null, String.class));
            paramList.add(ParamTool.getParam(12, null, String.class));

            //创建时间、更新时间默认值
            Date date = new Date();
            paramList.add(ParamTool.getParam(13, date, Date.class));
            paramList.add(ParamTool.getParam(14, date, Date.class));
            prepareStatement.setParamList(paramList);
            return prepareStatement;
        } else if(baseBean instanceof ProductBean){
            return this.getUpdatePrepareStatement(name, operationType, baseBean);
        } else if(baseBean instanceof ShipmentBean){
            return this.getUpdatePrepareStatement(name, operationType, baseBean);
        }
        return null;
    }

    private StatementBean getUpdatePrepareStatement(String name, String operationType, BaseBean baseBean) {
        PrepareStatementBean prepareStatement = new PrepareStatementBean();
        prepareStatement.setOperationType(operationType);
        prepareStatement.setDataSourceName(TableConfig.getTableConfig(name).getDataSourceName());
        if(baseBean instanceof OrderBean) {
            OrderBean orderBean = (OrderBean) baseBean;
            String sql = "update enriched_orders set order_date = ?, customer_name = ?, price = ?, product_id = ?, order_status = ? , update_time = ? where order_id = ?";
            prepareStatement.setSql(sql);

            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(1, orderBean.getOrder_date(), Date.class));
            paramList.add(ParamTool.getParam(2, orderBean.getCustomer_name(), String.class));
            paramList.add(ParamTool.getParam(3, orderBean.getPrice(), BigDecimal.class));
            paramList.add(ParamTool.getParam(4, orderBean.getProduct_id(), Long.class));
            paramList.add(ParamTool.getParam(5, orderBean.getOrder_status(), Integer.class));
            paramList.add(ParamTool.getParam(6, new Date(), Date.class));
            paramList.add(ParamTool.getParam(7, orderBean.getOrder_id(), Long.class));
//            paramList.add(ParamTool.getParam(orderBean.getOrder_date(), 1, Date.class));
//            paramList.add(ParamTool.getParam(orderBean.getCustomer_name(), 2, String.class));
//            paramList.add(ParamTool.getParam(orderBean.getPrice(), 3, BigDecimal.class));
//            paramList.add(ParamTool.getParam(orderBean.getProduct_id(), 4, Long.class));
//            paramList.add(ParamTool.getParam(orderBean.getOrder_status(), 5, Integer.class));
//            paramList.add(ParamTool.getParam(orderBean.getOrder_id(), 6, Long.class));
            prepareStatement.setParamList(paramList);

            return prepareStatement;
        } else if(baseBean instanceof ProductBean){
            ProductBean productBean = (ProductBean)baseBean;
            String sql = "update enriched_orders set name = ?, description = ?, update_time = ? where product_id = ?";
            prepareStatement.setSql(sql);

            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(1, productBean.getName(), String.class));
            paramList.add(ParamTool.getParam(2, productBean.getDescription(), String.class));
            paramList.add(ParamTool.getParam(3, new Date(), Date.class));
            paramList.add(ParamTool.getParam(4, productBean.getId(), Long.class));

//            paramList.add(ParamTool.getParam(productBean.getName(), 1, String.class));
//            paramList.add(ParamTool.getParam(productBean.getDescription(), 2, String.class));
//            paramList.add(ParamTool.getParam(productBean.getId(), 3, Long.class));
            prepareStatement.setParamList(paramList);

            return prepareStatement;
        } else if(baseBean instanceof ShipmentBean){
            ShipmentBean shipmentBean = (ShipmentBean)baseBean;
            String sql = "update enriched_orders set shipment_id = ?, origin = ?, destination = ?, is_arrived = ?, update_time = ? where order_id = ?";
            prepareStatement.setSql(sql);

            List<PrepareStatementBean.Param<?>> paramList = new ArrayList<>();
            paramList.add(ParamTool.getParam(1, shipmentBean.getShipment_id(), Long.class));
            paramList.add(ParamTool.getParam(2, shipmentBean.getOrigin(), String.class));
            paramList.add(ParamTool.getParam(3, shipmentBean.getDestination(), String.class));
            paramList.add(ParamTool.getParam(4, shipmentBean.getIs_arrived(), Integer.class));
            paramList.add(ParamTool.getParam(5, new Date(), Date.class));
            paramList.add(ParamTool.getParam(6, shipmentBean.getOrder_id(), Long.class));
            prepareStatement.setParamList(paramList);

            return prepareStatement;
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
