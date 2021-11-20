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
public class BpmProInstBean extends BaseBean{

    private String ID_;

    private String SUBJECT_;

    private String BIZ_KEY_;

    private String STATUS_;

    private Date END_TIME_;

    private Long DURATION_;

    private String PROC_DEF_KEY_;

    private String PROC_DEF_NAME_;

    private String PARENT_INST_ID_;
}
