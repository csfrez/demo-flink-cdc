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
public class BizIsrMixedBean extends BaseBean{

    private String apply_no;

    private String is_special_approved;

    private String is_priority;

    //materials_upload_status,tail_release_node,to_use_amount_flag,credit_unfinish,rule_level,feedback_on_abnormal_order_flag
    private String materials_upload_status;

    private String tail_release_node;

    private String to_use_amount_flag;

    private String credit_unfinish;

    private String rule_level;

    private String feedback_on_abnormal_order_flag;

}
