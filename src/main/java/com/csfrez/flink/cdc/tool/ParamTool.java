package com.csfrez.flink.cdc.tool;

import com.csfrez.flink.cdc.bean.PrepareStatementBean;

public class ParamTool {

    public static <T> PrepareStatementBean.Param<T> getParam(T value, int index, Class<T> clazz){
        PrepareStatementBean.Param<T> param = new PrepareStatementBean.Param<>();
        param.setIndex(index);
        param.setValue(value);
        param.setType(clazz);
        return param;
    }
}
