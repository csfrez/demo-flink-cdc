package com.csfrez.flink.cdc.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * @author yangzhi
 * @date 2021/11/15
 * @email csfrez@163.com
 */
@Getter
@Setter
public class BpmTaskCandidateBean extends BaseBean{

   private String ID_;

   private String TASK_ID_;

   private String TYPE_;

   private String EXECUTOR_;

   private String PROC_INST_ID_;
}
