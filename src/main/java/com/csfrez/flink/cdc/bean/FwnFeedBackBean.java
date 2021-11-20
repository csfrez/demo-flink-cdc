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
public class FwnFeedBackBean extends BaseBean{

    private String apply_no;

    private String status;

    private Date estimate_cancle_time;

    private String reason;

    private String solution;

}
