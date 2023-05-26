package com.datacenter.canal.load.support;

import java.util.List;
import java.util.stream.IntStream;

/**
 * SQL 拼接工具
 */
public class SqlBuilder {

    private final String backtick;
    private final StringBuilder builder;

    public SqlBuilder(String backtick) {
        this.backtick = backtick;
        this.builder = new StringBuilder();
    }

    public SqlBuilder(String sql, String backtick) {
        this.backtick = backtick;
        this.builder = new StringBuilder(sql);
    }

    public SqlBuilder append(String str) {
        builder.append(str);
        return this;
    }

    /**
     * 使用 Backtick 插入
     * ex: mysql - user => `user`, postgresql - user => "user"
     */
    public SqlBuilder appendWithBacktick(String str) {
        builder.append(backtick).append(str).append(backtick);
        return this;
    }

    /**
     * 重複插入
     */
    public SqlBuilder appendRepeat(int count, String str) {
        IntStream.range(0, count).forEach(i -> builder.append(str));
        return this;
    }

    /**
     * 將 List 合併後，使用 Backtick 插入
     */
    public SqlBuilder appendJoinWithBacktick(String delimiter, List<String> elements) {
        elements.forEach(e -> appendWithBacktick(e).append(delimiter));
        return this;
    }

    /**
     * 從最後刪除固定長度
     */
    public SqlBuilder deleteBehind(int num) {
        int length = builder.length();
        builder.delete(length - num, length);
        return this;
    }

    public String toString() {
        return builder.toString();
    }
}
