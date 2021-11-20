package com.csfrez.flink.cdc.bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProductBean extends BaseBean{

    private Long id;

    private String name;

    private String description;

}
