package com.datacenter.canal.load.support;

import com.datacenter.canal.select.support.EtlColumn;
import com.datacenter.canal.select.support.EtlMessage;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

/**
 * 單一 DML 紀錄檔
 */
@Getter
@Setter
public class SingleDml {
    private String type;
    private boolean isChange;
    private Map<String, EtlColumn> data;
    private Map<String, EtlColumn> old;
    private Set<String> dataKeys; // 變動或被變動資料的 key 集合，串接所有 PK 欄位的數值，用於計算優先權
    private Set<String> changedKeys; // update 時，被變動的欄位名稱集合
    private int priority; // 執行 SQL 的優先權，越低越優先

    public SingleDml(EtlMessage etlMessage, int index) {
        this.isChange = true;
        this.type = etlMessage.getType();
        this.data = etlMessage.getData().get(index);
        this.priority = 0;

        // 假如為 update
        if ("UPDATE".equalsIgnoreCase(this.type)) {
            this.old = etlMessage.getOld().get(index);

            // 檢查新舊資料有無變動
            generateChangedKeys();

            // 假如新舊資料無變動，則不必執行此 SQL
            if (this.changedKeys.isEmpty()) {
                this.isChange = false;
            }
        }

        // 產生變動或被變動資料的 key 集合
        generateDataKeys(etlMessage.getPkNames());
    }

    /**
     * 比對變動或被變動資料的 key 集合有無交集
     */
    public boolean compareDataKeys(Set<String> comparedDataKeys) {
        for (String key : this.dataKeys) {
            if (comparedDataKeys.contains(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 依 SQL 類別，增加優先權
     */
    public void addTypePriority() {
        switch (this.type) {
            case "INSERT":
                this.priority += 1;
                return;
            case "UPDATE":
                this.priority += 2;
                return;
            case "DELETE":
                this.priority += 3;
        }
    }

    /**
     * 產生變動或被變動資料的 key 集合，串接所有 PK 欄位的數值，用於計算優先權
     */
    private void generateDataKeys(List<String> pkNames) {
        this.dataKeys = new HashSet<>();

        StringBuilder key = new StringBuilder();
        for (String pkName : pkNames) {
            Object value = this.data.get(pkName).getValue();
            key.append(value).append("+!#");
        }
        this.dataKeys.add(key.toString());

        // 假如為 update，需要額外產生被變動的資料 key
        if ("UPDATE".equalsIgnoreCase(this.type)) {
            key = new StringBuilder();

            for (String pkName : pkNames) {
                Object value = this.old.get(pkName).getValue();
                key.append(value).append("+!#");
            }
            this.dataKeys.add(key.toString());
        }
    }

    /**
     * 比對新舊資料，尋找有變動的欄位
     */
    private void generateChangedKeys() {
        this.changedKeys = new HashSet<>();
        this.data.values().forEach(column -> {
            String columnName = column.getName();
            EtlColumn oldColumn = old.get(columnName);

            if (oldColumn == null || !Objects.equals(column.getValue(), oldColumn.getValue())) {
                this.changedKeys.add(columnName);
            }
        });
    }
}
