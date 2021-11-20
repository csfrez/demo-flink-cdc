package com.csfrez.flink.cdc.bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ShipmentBean extends BaseBean{

    private Long shipment_id;

    private Long order_id;

    private String origin;

    private String destination;

    private Integer is_arrived;

}
