package com.csfrez.flink.cdc.bean;

import lombok.Data;

import java.util.Date;

@Data
public class OrderBean extends BaseBean{

    private Long order_id;

    private Date order_date;

    private String customer_name;

    private String price;

    private Long product_id;

    private Integer order_status;

}
