package com.datacenter.canal.load;

import com.alibaba.druid.pool.DruidDataSource;
import com.datacenter.canal.load.support.BatchExecutor;
import com.datacenter.canal.load.support.SqlBuilder;
import com.datacenter.canal.load.support.SyncUtil;
import com.datacenter.canal.select.support.EtlColumn;
import com.datacenter.canal.select.support.EtlMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LoadService {

    @Autowired
    DataSource dataSource;

    public void load(List<EtlMessage> messages) throws SQLException {
        log.debug("do load process");

        BatchExecutor batchExecutor = new BatchExecutor(dataSource);

        for (EtlMessage message : messages) {
            log.debug("load process for binlog{}-{}: {} table {}", message.getLogfileName(), message.getLogfileOffset(), message.getType(), message.getTable());

            // 檢查有無資料
            if (message.getData().isEmpty()) {
                continue;
            }

            // 依據事件類型執行不同的 SQL
            switch (message.getType()) {
                case "INSERT":
                    insert(batchExecutor, message);
                    break;
                case "UPDATE":
                    update(batchExecutor, message);
                    break;
                case "DELETE":
                    delete(batchExecutor, message);
                    break;
                case "TRUNCATE":
                    // TRUNCATE 屬於 DDL 且 message.getData() 為空，正常情況不會觸發
                    truncate(batchExecutor, message);
                    break;
                default:
                    log.warn("Unsupported event type: {}, sql: {}", message.getType(), message.getSql());
                    break;
            }
        }

        try {
            batchExecutor.commit(); // 提交所有的 SQL
        } catch (Throwable e) {
            batchExecutor.rollback();
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作
     */
    private void insert(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
        List<String> columnNames = new ArrayList<>(message.getData().get(0).keySet());
        List<Map<String, ?>> values = new ArrayList<>();
        String backtick = getBacktick();

        // 組成 SQL
        SqlBuilder sql = new SqlBuilder(backtick);
        sql.append("INSERT INTO ").appendWithBacktick(message.getTable())
                .append(" (").appendJoinWithBacktick(",", columnNames).deleteBehind(1) // 插入欄位
                .append(") VALUES ");

        // 插入數值，一口氣插入多筆
        for (Map<String, EtlColumn> data : message.getData()) {
            sql.append("(").appendRepeat(data.size(), "?,").deleteBehind(1).append("),");

            columnNames.forEach(columnName -> {
                EtlColumn column = data.get(columnName);
                BatchExecutor.setValue(values, column.getSqlType(), column.getValue());
            });
        }

        sql.deleteBehind(1); // 移除多餘的逗點

        batchExecutor.execute(sql.toString(), values); // 執行 SQL (尚未提交)

        if (log.isTraceEnabled()) {
            log.trace("Insert into target table, sql: {}", sql);
        }
    }

    /**
     * 更新操作
     */
    private void update(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
        // 取得 PK
        Set<String> keys = getKeys(message);

        // 依數值分別執行 SQL
        for (int i = 0; i < message.getData().size(); i++) {
            update(batchExecutor, message.getTable(), keys, message.getData().get(i), message.getOld().get(i));
        }
    }

    /**
     * 更新操作
     */
    private void update(BatchExecutor batchExecutor, String tableName, Set<String> keys, Map<String, EtlColumn> data, Map<String, EtlColumn> old) throws SQLException {
        // 檢查有無資料變更
        List<EtlColumn> changedColumns = getChangedColumns(data, old);
        if (changedColumns.isEmpty()) {
            return;
        }

        List<EtlColumn> keyColumns = getKeyColumns(keys, old); // 從舊資料取得變更前的欄位數值
        List<Map<String, ?>> values = new ArrayList<>();
        String backtick = getBacktick();

        // 組成 SQL
        SqlBuilder sql = new SqlBuilder(backtick);
        sql.append("UPDATE ").appendWithBacktick(tableName).append(" SET ");

        // 僅更新有變動的欄位
        for (EtlColumn changedColumn : changedColumns) {
            sql.appendWithBacktick(changedColumn.getName()).append("=?, ");
            BatchExecutor.setValue(values, changedColumn.getSqlType(), changedColumn.getValue());
        }

        sql.deleteBehind(2).append(" WHERE ");

        // 使用 PK & 舊資料的數值作為更新條件
        for (EtlColumn keyColumn : keyColumns) {
            sql.appendWithBacktick(keyColumn.getName()).append("=? AND ");
            BatchExecutor.setValue(values, keyColumn.getSqlType(), keyColumn.getValue());
        }

        sql.deleteBehind(4); // 移除多餘的 AND

        batchExecutor.execute(sql.toString(), values); // 執行 SQL (尚未提交)

        if (log.isTraceEnabled()) {
            log.trace("Update target table, sql: {}", sql);
        }
    }

    /**
     * 尋找資料內容有變化的欄位
     */
    private List<EtlColumn> getChangedColumns(Map<String, EtlColumn> columns, Map<String, EtlColumn> oldColumns) {
        List<EtlColumn> changedColumns = new ArrayList<>();

        columns.values().forEach(column -> {
            EtlColumn oldColumn = oldColumns.get(column.getName());

            if (oldColumn == null || !Objects.equals(column.getValue(), oldColumn.getValue())) {
                changedColumns.add(column);
            }
        });

        return changedColumns;
    }

    /**
     * 删除操作
     */
    private void delete(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
        // 取得 PK
        Set<String> keys = getKeys(message);

        // 依數值分別執行 SQL
        for (int i = 0; i < message.getData().size(); i++) {
            delete(batchExecutor, message.getTable(), keys, message.getData().get(i));
        }
    }

    /**
     * 删除操作
     */
    private void delete(BatchExecutor batchExecutor, String tableName, Set<String> keys, Map<String, EtlColumn> data) throws SQLException {
        List<Map<String, ?>> values = new ArrayList<>();
        String backtick = getBacktick();

        List<EtlColumn> keyColumns = getKeyColumns(keys, data); // 取得刪除前的欄位數值

        // 組成 SQL
        SqlBuilder sql = new SqlBuilder(backtick);
        sql.append("DELETE FROM ").appendWithBacktick(tableName).append(" WHERE ");

        // 使用 PK & 舊資料的數值作為更新條件
        for (EtlColumn keyColumn : keyColumns) {
            sql.appendWithBacktick(keyColumn.getName()).append("=? AND ");
            BatchExecutor.setValue(values, keyColumn.getSqlType(), keyColumn.getValue());
        }

        sql.deleteBehind(4); // 移除多餘的 AND

        batchExecutor.execute(sql.toString(), values); // 執行 SQL (尚未提交)

        if (log.isTraceEnabled()) {
            log.trace("Delete from target table, sql: {}", sql);
        }
    }

    /**
     * truncate操作
     */
    private void truncate(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
        String backtick = getBacktick();

        // 組成 SQL
        SqlBuilder sql = new SqlBuilder(backtick);
        sql.append("TRUNCATE TABLE ").appendWithBacktick(message.getTable());

        batchExecutor.execute(sql.toString(), new ArrayList<>());

        if (log.isTraceEnabled()) {
            log.trace("Truncate target table, sql: {}", sql);
        }
    }

    /**
     * 依據資料來源，取得 Backtick
     */
    private String getBacktick() {
        DruidDataSource druidDataSource = (DruidDataSource) dataSource;
        return SyncUtil.getBacktickByDbType(druidDataSource.getDbType());
    }

    /**
     * 取得 primary key
     */
    private Set<String> getKeys(EtlMessage message) {
        return message.getData().get(0).values()
                .stream().filter(EtlColumn::isKey).map(EtlColumn::getName)
                .collect(Collectors.toSet());
    }

    /**
     * 取得 key columns
     */
    private List<EtlColumn> getKeyColumns(Set<String> keys, Map<String, EtlColumn> data) {
        return data.values()
                .stream().filter(c -> keys.contains(c.getName()))
                .collect(Collectors.toList());
    }
}
