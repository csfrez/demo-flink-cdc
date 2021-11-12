package com.csfrez.flink.cdc.service;

import com.csfrez.flink.cdc.bean.BaseBean;
import com.csfrez.flink.cdc.bean.StatementBean;

public interface ProcessService {

    StatementBean process(String name, String operationType, BaseBean baseBean);

}
