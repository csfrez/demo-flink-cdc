package com.csfrez.flink.cdc.bean;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * @author yangzhi
 * @date 2021/11/15
 * @email csfrez@163.com
 */
@Getter
@Setter
public class BpmCheckOpinionBean extends BaseBean{

   private String ID_;

   private String PROC_INST_ID_;

   private String AUDITOR_;

   private String AUDITOR_NAME_;

   private String TASK_KEY_;

   private String TASK_NAME_;

   private Date CREATE_TIME_;

   private String TASK_ID_;

   private String PROC_DEF_ID_;

   private Date COMPLETE_TIME_;
}
