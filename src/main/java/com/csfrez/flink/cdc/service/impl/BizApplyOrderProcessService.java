package com.csfrez.flink.cdc.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.BinlogBean;
import com.csfrez.flink.cdc.service.ProcessService;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email yangzhi@ddjf.com.cn
 */
public class BizApplyOrderProcessService implements ProcessService {

    @Override
    public void process(JSONObject before, JSONObject after, BinlogBean.Source source) {
        System.out.println(before);
        System.out.println(after);
    }
}
