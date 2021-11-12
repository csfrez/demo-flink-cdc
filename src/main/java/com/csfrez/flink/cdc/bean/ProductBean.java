package com.csfrez.flink.cdc.bean;

import lombok.Data;

@Data
public class ProductBean extends BaseBean{

    private Long id;

    private String name;

    private String description;

}
