package com.datacenter.canal.select.support;

import lombok.*;

@Getter
@Setter
@Builder
@ToString
public class EtlColumn {
    private String name;
    private Object value;
    private boolean key;
    private int sqlType;
    private String mysqlType;
}
