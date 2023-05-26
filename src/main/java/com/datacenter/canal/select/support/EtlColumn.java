package com.datacenter.canal.select.support;

import lombok.*;

/**
 * modified from canal-connect
 */
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
