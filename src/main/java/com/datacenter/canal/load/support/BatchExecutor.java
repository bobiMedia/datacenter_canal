package com.datacenter.canal.load.support;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BatchExecutor implements Closeable {

    private final AtomicInteger idx = new AtomicInteger(0);
    private final DataSource dataSource;
    private Connection conn;

    @Getter
    private String backtick;

    public BatchExecutor(DataSource dataSource) throws SQLException {
        this.dataSource = dataSource;
        this.backtick = getBacktick(dataSource);
    }

    public Connection getConn() {
        if (conn == null) {
            try {
                conn = dataSource.getConnection();
                this.conn.setAutoCommit(false);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
        return conn;
    }

    public static void setValue(List<Map<String, ?>> values, int type, Object value) {
        Map<String, Object> valueItem = new HashMap<>();
        valueItem.put("type", type);
        valueItem.put("value", value);
        values.add(valueItem);
    }

    public void execute(String sql, List<Map<String, ?>> values) throws SQLException {
        PreparedStatement ps = getConn().prepareStatement(sql);
        int len = values.size();
        for (int i = 0; i < len; i++) {
            int type = (Integer) values.get(i).get("type");
            Object value = values.get(i).get("value");
            SyncUtil.setPStmt(type, ps, value, i + 1);
        }

        ps.execute();
        idx.incrementAndGet();
        ps.close();
    }

    public void commit() throws SQLException {
        getConn().commit();
        if (log.isTraceEnabled()) {
            log.trace("Batch executor commit " + idx.get() + " rows");
        }
        idx.set(0);
    }

    public void rollback() throws SQLException {
        getConn().rollback();
        if (log.isTraceEnabled()) {
            log.trace("Batch executor rollback " + idx.get() + " rows");
        }
        idx.set(0);
    }

    @Override
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            } finally {
                conn = null;
            }
        }
    }

    /**
     * 依據資料來源，取得 Backtick
     */
    private static String getBacktick(DataSource dataSource) throws SQLException {
        if(dataSource instanceof HikariDataSource) {
            HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
            return SyncUtil.getBacktickByUrl(hikariDataSource.getJdbcUrl());
        }

        log.warn("Can not detect dataSource type");
        return "";
    }
}
