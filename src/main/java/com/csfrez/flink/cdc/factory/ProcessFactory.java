package com.csfrez.flink.cdc.factory;


import com.csfrez.flink.cdc.service.impl.BizApplyOrderProcessService;
import com.csfrez.flink.cdc.service.ProcessService;

/**
 * @author yangzhi
 * @date 2021/11/11
 * @email yangzhi@ddjf.com.cn
 */
public class ProcessFactory {

    public static ProcessService createProcess(String name) {
        if (name.equalsIgnoreCase("bpms.biz_apply_order")) {
            return new BizApplyOrderProcessService();
        } else if (name.equalsIgnoreCase("bpms.biz_fee_summary")) {
            return new BizApplyOrderProcessService();
        } else {
            System.out.println("Invalid factory name");
            return null;
        }
    }
}
