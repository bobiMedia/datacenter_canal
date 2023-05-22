package com.datacenter.canal.select.support;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class EtlMessage  implements Serializable {

    private static final long         serialVersionUID = 2611556444074013268L;

    private String                    database;                               // 数据库或schema
    private String                    table;                                  // 表名
    private List<String> pkNames;
    private Boolean                   isDdl;
    // 类型:INSERT/UPDATE/DELETE
    private String                    type;
    // binlog executeTime, 执行耗时
    private Long                      es;
    // dml build timeStamp, 同步时间
    private Long                      ts;
    // 执行的sql,dml sql为空
    private String                    sql;
    // 数据列表
    private List<Map<String, EtlColumn>> data;
    // 旧数据列表,用于update,size和data的size一一对应
    private List<Map<String, EtlColumn>> old;
    private String logfileName;
    private Long logfileOffset;
}
