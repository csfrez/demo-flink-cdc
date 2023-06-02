package com.csfrez.flink.cdc.bean;


import com.alibaba.fastjson2.JSONObject;

import java.io.Serializable;

public class BaseBean implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}
