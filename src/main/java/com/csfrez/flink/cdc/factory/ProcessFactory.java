package com.csfrez.flink.cdc.factory;


import com.csfrez.flink.cdc.service.impl.BizApplyOrderProcessService;
import com.csfrez.flink.cdc.service.ProcessService;
import com.csfrez.flink.cdc.service.impl.EnrichedOrdersProcessService;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email yangzhi@ddjf.com.cn
 */
public class ProcessFactory {

    public static ProcessService createProcess(String name) {
        if (name.equalsIgnoreCase("bpms.biz_apply_order")) {
            return new BizApplyOrderProcessService();
        } else if (name.equalsIgnoreCase("mydb.enriched_orders")) {
            return new EnrichedOrdersProcessService();
        } else {
            System.out.println("Invalid factory name =====>>" + name);
            return null;
        }
    }
}
