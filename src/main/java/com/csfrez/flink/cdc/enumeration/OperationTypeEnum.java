package com.csfrez.flink.cdc.enumeration;

public enum OperationTypeEnum {

    INSERT("insert", "新增"), UPDATE("update", "更新"), DELETE("delete", "删除");
    private String value;
    private String name;

    private OperationTypeEnum(String value, String name){
        this.value = value;
        this.name = name;
    }

    //覆盖方法
    @Override
    public String toString() {
        return this.value+"_"+this.name;
    }

    public String value(){
        return this.value;
    }
}
