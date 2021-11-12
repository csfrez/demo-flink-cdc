package com.csfrez.flink.cdc.service;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.BinlogBean;

public interface ProcessService {

    void process(JSONObject before, JSONObject after, BinlogBean.Source source);

}
