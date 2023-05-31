package com.datacenter.canal.load;

import com.datacenter.canal.load.support.BatchExecutor;
import com.datacenter.canal.load.support.SingleDml;
import com.datacenter.canal.load.support.SqlBuilder;
import com.datacenter.canal.load.support.SyncUtil;
import com.datacenter.canal.select.support.EtlColumn;
import com.datacenter.canal.select.support.EtlMessage;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Service
public class LoadService {

    private static final int MERGE_INSERT_SIZE = 200;
    private static final int BATCH_UPDATE_SIZE = 500;
    private static final int BATCH_DELETE_SIZE = 1000;

    @Autowired
    DataSource dataSource;

    @Value("${canal.target.suffix:}")
    String tableSuffix;

    private BatchExecutor batchExecutor;

    @PostConstruct
    private void init() throws SQLException {
        log.trace("Init LoadService: MERGE_INSERT_SIZE: {}, BATCH_UPDATE_SIZE: {}, BATCH_DELETE_SIZE: {}",
                MERGE_INSERT_SIZE, BATCH_UPDATE_SIZE, BATCH_DELETE_SIZE);
        this.batchExecutor = new BatchExecutor(dataSource);
    }

    public void load(List<EtlMessage> messages) throws SQLException {
        log.debug("do load process");

        // 將所有 Message 轉為單一 DML 語句
        List<SingleDml> totalDml = messages.stream().flatMap(etlMessage ->
                IntStream.range(0, etlMessage.getData().size()).mapToObj(i -> new SingleDml(etlMessage, i))
        ).filter(SingleDml::isChange).collect(Collectors.toList());
        log.debug("load process total sql size: {}", totalDml.size());

        // 檢查所有 DML 語句使用到的 PK 值，並排定優先度
        for (int i = 0; i < totalDml.size(); i++) {
            SingleDml dml = totalDml.get(i);
            for (int j = i - 1; j >= 0; j--) {
                SingleDml comparedDml = totalDml.get(j);
                if (dml.compareDataKeys(comparedDml.getDataKeys())) {
                    dml.setPriority(comparedDml.getPriority() + 10);
                    break;
                }
            }
        }

        // 根據 SQL 的類型，修正優先度，INSERT -> UPDATE -> DELETE
        totalDml.forEach(SingleDml::addTypePriority);

        // 根據優先度切分
        Map<Integer, List<SingleDml>> priorityMap = totalDml.stream().collect(Collectors.groupingBy(SingleDml::getPriority));
        log.debug("load process priority count: {}", priorityMap.size());

        // 依據優先度執行
        List<Integer> priorityIds = priorityMap.keySet().stream().sorted().collect(Collectors.toList());
        for (Integer priorityId : priorityIds) {
            List<SingleDml> dmlList = priorityMap.get(priorityId);

            if (dmlList.size() > 0) {
                String table = messages.get(0).getTable();
                List<String> pkNames = messages.get(0).getPkNames();

                if(!tableSuffix.isEmpty()) {
                    table += tableSuffix;
                }

                // 依據事件類型執行不同的 SQL
                switch (dmlList.get(0).getType()) {
                    case "INSERT":
                        log.trace("load process do insert");
                        insert(table, dmlList, batchExecutor);
                        break;
                    case "UPDATE":
                        log.trace("load process do update");
                        update(table, pkNames, dmlList, batchExecutor);
                        break;
                    case "DELETE":
                        log.trace("load process do delete");
                        delete(table, pkNames, dmlList, batchExecutor);
                        break;
                    default:
                        log.warn("Unsupported event type: {}", dmlList.get(0).getType());
                        break;
                }
            }
        }

        try {
            log.trace("Start commit");
            batchExecutor.commit(); // 提交所有的 SQL
            log.trace("End commit");
        } catch (Throwable e) {
            batchExecutor.rollback();
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作
     */
    private void insert(String table, List<SingleDml> dmlList, BatchExecutor batchExecutor) throws SQLException {
        // 取得所有欄位
        List<String> columnNames = new ArrayList<>(dmlList.get(0).getData().keySet());

        // 串接基本 INSERT SQL (到 value 為止), ex: INSERT INTO "table" ("col_1", "col_2", "col_3") VALUES
        String sqlPrefix = new SqlBuilder(batchExecutor.getBacktick()).append("INSERT INTO ").appendWithBacktick(table)
                .append(" (").appendJoinWithBacktick(",", columnNames).deleteBehind(1) // 插入欄位
                .append(") VALUES ").toString();

        // 串接 INSERT SQL (value 部分), ex: (?,?,?),
        String sqlValue = new SqlBuilder(batchExecutor.getBacktick())
                .append("(").appendRepeat(columnNames.size(), "?,").deleteBehind(1).append("),").toString();

        // 完整合併 INSERT SQL (待串接)
        SqlBuilder sql;

        // 依據預設合併大小切割 DML
        List<List<SingleDml>> mergeDmlList = Lists.partition(dmlList, MERGE_INSERT_SIZE);
        List<SingleDml> lastMergeDml = mergeDmlList.get(mergeDmlList.size() - 1);

        if (mergeDmlList.size() > 1 || lastMergeDml.size() == MERGE_INSERT_SIZE) {
            // 串接合併 INSERT SQL (for 預設大小)
            sql = new SqlBuilder(sqlPrefix, batchExecutor.getBacktick())
                    .appendRepeat(MERGE_INSERT_SIZE, sqlValue).deleteBehind(1);

            // 使用批次插入多筆合併 INSERT SQL (for 預設大小)
            List<List<Map<String, ?>>> batchValues = new ArrayList<>();
            mergeDmlList.stream().filter(l -> l.size() == MERGE_INSERT_SIZE).forEach(mergeDml -> {
                List<Map<String, ?>> values = new ArrayList<>();
                for (SingleDml dml : mergeDml) {
                    columnNames.forEach(columnName -> {
                        EtlColumn column = dml.getData().get(columnName);
                        BatchExecutor.setValue(values, column.getSqlType(), column.getValue());
                    });
                }
                batchValues.add(values);
            });

            // 批次執行 SQL (尚未提交)
            batchExecutor.executeBatch(sql.toString(), batchValues);

            if (log.isTraceEnabled()) {
                log.trace("Batch insert into target table, count: {}, sql: {}", batchValues.size(), sql);
            }
        }

        // 檢查是否有小於預設合併大小的 DML，若有則重組 SQL，單獨執行一遍
        if (lastMergeDml.size() != MERGE_INSERT_SIZE) {
            sql = new SqlBuilder(sqlPrefix, batchExecutor.getBacktick())
                    .appendRepeat(lastMergeDml.size(), sqlValue).deleteBehind(1);
            List<Map<String, ?>> values = new ArrayList<>();

            for (SingleDml dml : lastMergeDml) {
                Map<String, EtlColumn> data = dml.getData();

                columnNames.forEach(columnName -> {
                    EtlColumn column = data.get(columnName);
                    BatchExecutor.setValue(values, column.getSqlType(), column.getValue());
                });
            }

            // 執行 SQL (尚未提交)
            batchExecutor.execute(sql.toString(), values);

            if (log.isTraceEnabled()) {
                log.trace("Insert into target table, sql: {}", sql);
            }
        }
    }

    /**
     * 更新操作
     */
    private void update(String tableName, List<String> pkNames, List<SingleDml> dmlList, BatchExecutor batchExecutor) throws SQLException {
        // 串接基本 UPDATE SQL (到 set 為止), ex: UPDATE "table" SET
        String sqlPrefix = new SqlBuilder(batchExecutor.getBacktick())
                .append("UPDATE ").appendWithBacktick(tableName).append(" SET ").toString();

        // 串接 UPDATE SQL (where 部分), ex: WHERE "col_1" =? AND "col_2" =? AND "col_3"=?
        String sqlWhere = new SqlBuilder(batchExecutor.getBacktick())
                .append(" WHERE ").appendJoinWithBacktick("=? AND ", pkNames).deleteBehind(4).toString();

        // 依據預設批次大小切割 DML
        List<List<SingleDml>> batches = Lists.partition(dmlList, BATCH_UPDATE_SIZE);
        for (List<SingleDml> batch : batches) {
            // 合併被修改的欄位，取該批次所有 DML 修改欄位的聯集
            List<String> changedKeys = new ArrayList<>(batch.stream().map(SingleDml::getChangedKeys)
                    .reduce(new HashSet<>(), SyncUtil::mergeSet));

            // 串接該批次完整的 UPDATE SQL
            SqlBuilder sql = new SqlBuilder(sqlPrefix, batchExecutor.getBacktick())
                    .appendJoinWithBacktick("=?, ", changedKeys).deleteBehind(2)
                    .append(sqlWhere);

            // 使用批次更新多筆資料
            List<List<Map<String, ?>>> batchValues = new ArrayList<>();
            for (SingleDml dml : batch) {
                List<Map<String, ?>> values = new ArrayList<>();

                // 設定更改欄位的數值
                for (String changedKey : changedKeys) {
                    EtlColumn column = dml.getData().get(changedKey);
                    BatchExecutor.setValue(values, column.getSqlType(), column.getValue());
                }

                // 設定條件欄位的數值
                for (String pkName : pkNames) {
                    EtlColumn column = dml.getData().get(pkName);
                    BatchExecutor.setValue(values, column.getSqlType(), column.getValue());
                }

                batchValues.add(values);
            }

            // 批次執行 SQL (尚未提交)
            batchExecutor.executeBatch(sql.toString(), batchValues);

            if (log.isTraceEnabled()) {
                log.trace("Batch update target table, sql: {}", sql);
            }
        }
    }

    /**
     * 删除操作
     */
    private void delete(String tableName, List<String> pkNames, List<SingleDml> dmlList, BatchExecutor batchExecutor) throws SQLException {
        // 串接 DELETE SQL
        String sql = new SqlBuilder(batchExecutor.getBacktick())
                .append("DELETE FROM ").appendWithBacktick(tableName)
                .append(" WHERE ").appendJoinWithBacktick("=? AND ", pkNames).deleteBehind(4).toString();

        // 依據預設批次大小切割 DML
        List<List<SingleDml>> batches = Lists.partition(dmlList, BATCH_DELETE_SIZE);
        for (List<SingleDml> batch : batches) {

            // 使用批次刪除多筆資料
            List<List<Map<String, ?>>> batchValues = new ArrayList<>();
            for (SingleDml dml : batch) {
                List<Map<String, ?>> values = new ArrayList<>();

                // 設定條件欄位的數值
                for (String pkName : pkNames) {
                    EtlColumn column = dml.getData().get(pkName);
                    BatchExecutor.setValue(values, column.getSqlType(), column.getValue());
                }

                batchValues.add(values);
            }

            // 批次執行 SQL (尚未提交)
            batchExecutor.executeBatch(sql, batchValues);

            if (log.isTraceEnabled()) {
                log.trace("Batch delete target table, sql: {}", sql);
            }
        }
    }
}
