package com.csfrez.flink.cdc.tool;

import com.csfrez.flink.cdc.bean.PrepareStatementBean;

import java.util.function.Supplier;

public class ParamTool {

    @Deprecated
    public static <T> PrepareStatementBean.Param<T> getParam(T value, int index, Class<T> clazz){
        PrepareStatementBean.Param<T> param = new PrepareStatementBean.Param<>();
        param.setIndex(index);
        param.setValue(value);
        param.setType(clazz);
        return param;
    }

    public static <T> PrepareStatementBean.Param<T> getParam(int index, T value, Class<T> clazz){
        PrepareStatementBean.Param<T> param = new PrepareStatementBean.Param<>();
        param.setIndex(index);
        param.setValue(value);
        param.setType(clazz);
        return param;
    }

    public static <T> PrepareStatementBean.Param<T> getParamSupplier(int index, Supplier<T> supplier, Class<T> clazz){
        PrepareStatementBean.Param<T> param = new PrepareStatementBean.Param<>();
        param.setIndex(index);
        if(supplier != null){
            param.setValue(supplier.get());
        }
        param.setType(clazz);
        return param;
    }
}
