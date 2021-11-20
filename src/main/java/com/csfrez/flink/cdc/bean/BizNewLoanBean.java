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
public class BizNewLoanBean extends BaseBean{

    private String house_no;

    private String new_loan_bank_name;

}
