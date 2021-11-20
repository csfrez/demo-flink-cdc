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
public class FwnTaskBean extends BaseBean{

    private String id;

    private String apply_no;

    private Integer status;

    private Date create_time;

}
