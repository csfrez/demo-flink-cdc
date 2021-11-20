package com.csfrez.flink.cdc.bean;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class PrepareStatementBean extends StatementBean{

    private List<Param<?>> paramList;

    @Data
    public static class Param<T> {

        private Integer index;

        private T value;

        private Class<T> type;

    }
}
