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
public class BpmTaskBean extends BaseBean{

    private String ID_;

    private String NAME_;

    private String SUBJECT_;

    private String PROC_INST_ID_;

    private String PROC_DEF_KEY_;

    private String PROC_DEF_NAME_;

    private String OWNER_ID_;

    private String ASSIGNEE_ID_;

    private String STATUS_;

    private Integer PRIORITY_;

    private Date DUE_TIME_;

    private Integer SUSPEND_STATE_;

    private String OWNER_NAME_;

    private String TASK_STATUS;

    private Date CREATE_TIME_;

    private String NODE_ID_;

    private String PROC_DEF_ID_;

    private String TASK_TYPE_;


}
