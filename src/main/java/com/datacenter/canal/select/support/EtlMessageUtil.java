package com.datacenter.canal.select.support;

import com.alibaba.otter.canal.connector.core.util.JdbcTypeUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.util.*;

public class EtlMessageUtil {

    public static List<EtlMessage> convert(Message message) {
        if (message == null) {
            return null;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<EtlMessage> msgs = new ArrayList<>(entries.size());
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            final EtlMessage msg = new EtlMessage();
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setDatabase(entry.getHeader().getSchemaName());
            msg.setTable(entry.getHeader().getTableName());
            msg.setType(eventType.toString());
            msg.setEs(entry.getHeader().getExecuteTime());
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setTs(System.currentTimeMillis());
            msg.setSql(rowChange.getSql());
            msgs.add(msg);
            List<Map<String, EtlColumn>> data = new ArrayList<>();
            List<Map<String, EtlColumn>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                msg.setPkNames(new ArrayList<>());
                int i = 0;
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                            && eventType != CanalEntry.EventType.DELETE) {
                        continue;
                    }

                    Map<String, EtlColumn> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;

                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }

                    for (CanalEntry.Column column : columns) {
                        if (i == 0) {
                            if (column.getIsKey()) {
                                msg.getPkNames().add(column.getName());
                            }
                        }
                        if (column.getIsNull()) {
                            row.put(column.getName(), covertToEtlColumn(column));
                        } else {
                            row.put(column.getName(), covertToEtlColumn(msg.getTable(), column));
                        }
                        // 获取update为true的字段
                        if (column.getUpdated()) {
                            updateSet.add(column.getName());
                        }
                    }
                    if (!row.isEmpty()) {
                        data.add(row);
                    }

                    if (eventType == CanalEntry.EventType.UPDATE) {
                        Map<String, EtlColumn> rowOld = new LinkedHashMap<>();
                        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                            if (updateSet.contains(column.getName())) {
                                if (column.getIsNull()) {
                                    rowOld.put(column.getName(), covertToEtlColumn(column));
                                } else {
                                    rowOld.put(column.getName(), covertToEtlColumn(msg.getTable(), column));
                                }
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }

                    i++;
                }
                if (!data.isEmpty()) {
                    msg.setData(data);
                }
                if (!old.isEmpty()) {
                    msg.setOld(old);
                }
            }
        }

        return msgs;
    }

    private static EtlColumn covertToEtlColumn(CanalEntry.Column entryColumn) {
        return EtlColumn.builder()
                .name(entryColumn.getName())
                .key(entryColumn.getIsKey())
                .mysqlType(entryColumn.getMysqlType())
                .sqlType(entryColumn.getSqlType())
                .build();
    }

    private static EtlColumn covertToEtlColumn(String table, CanalEntry.Column entryColumn) {
        Object value = JdbcTypeUtil.typeConvert(table,
                entryColumn.getName(),
                entryColumn.getValue(),
                entryColumn.getSqlType(),
                entryColumn.getMysqlType());

        EtlColumn column = covertToEtlColumn(entryColumn);
        column.setValue(value);

        return column;
    }
}
