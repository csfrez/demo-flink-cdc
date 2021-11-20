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
public class FwnWarnHistoryBean extends BaseBean{
    private String id;

    private String apply_no;

    private String warn_level; //product_due_time,duration,warn_level,feedback_time,warn_status

    private Date product_due_time;

    private Integer duration;

    private Date feedback_time;

    private Integer warn_status;


}
