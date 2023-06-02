package com.csfrez.flink.cdc.factory;


import com.csfrez.flink.cdc.service.ProcessService;
import com.csfrez.flink.cdc.service.impl.EnrichedOrdersProcessService;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email csfrez@163.com
 */
@Slf4j
public class ProcessFactory {

    private static final Map<String, ProcessService> serviceMap = new ConcurrentHashMap<>();

    static {
        try{
            serviceMap.put("mydb.enriched_orders", new EnrichedOrdersProcessService());
        } catch (Exception e){
            e.printStackTrace();
            log.error("init.ProcessFactory", e);
        }
    }

    public static ProcessService createProcess(String name) {
        ProcessService processService = serviceMap.get(name);
        if(processService != null){
            return processService;
        } else {
            log.info("Invalid factory name ====>>" + name);
            return null;
        }
    }
}
