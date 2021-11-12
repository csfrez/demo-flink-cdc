package com.csfrez.flink.cdc.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.csfrez.flink.cdc.bean.BaseBean;
import com.csfrez.flink.cdc.bean.BinlogBean;
import com.csfrez.flink.cdc.bean.StatementBean;
import com.csfrez.flink.cdc.service.ProcessService;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email yangzhi@ddjf.com.cn
 */
public class BizApplyOrderProcessService implements ProcessService {

    @Override
    public StatementBean process(String name, String operationType, BaseBean baseBean) {
        return null;
    }
}
