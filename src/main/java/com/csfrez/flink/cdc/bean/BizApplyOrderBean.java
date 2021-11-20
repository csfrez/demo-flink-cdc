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
public class BizApplyOrderBean extends BaseBean{

    private String apply_no;

    private String product_id;

    private String product_name;

    private String sales_user_name;

    private String apply_status;

    private String seller_name;

    private String rob_user_name;

    private String branch_id;

    private String partner_insurance_name;

    private String after_loan_status;

    private String order_rank;

    private Integer multi_loan_cnt;

    private String relate_type;

    private String delete_flag;

    private String group_apply_no;

    private String extra_group_apply_no;

    private String house_no;

    private String relate_apply_no;

    private String buyer_name;

    private Date apply_time;

    private String flow_instance_id;


}
