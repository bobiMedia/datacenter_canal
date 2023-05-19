package com.datacenter.canal.load;

import com.datacenter.canal.load.support.BatchExecutor;
import com.datacenter.canal.select.support.EtlMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class LoadService {

    @Autowired
    DataSource dataSource;

    public void load(List<EtlMessage> messages) throws SQLException {
        log.debug("do load");

        BatchExecutor batchExecutor = new BatchExecutor(dataSource);

        for (EtlMessage message : messages) {
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
                    truncate(batchExecutor, message);
                    break;
                default:
                    log.warn("Unsupported event type: {}, sql: {}", message.getType(), message.getSql());
                    break;
            }
        }

        try {
            batchExecutor.commit();
        } catch (Throwable e) {
            batchExecutor.rollback();
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作
     */
    private void insert(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
    }

    private void insert(BatchExecutor batchExecutor, EtlMessage message, Map<String, Object> data) throws SQLException {
    }

    private void update(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
    }

    /**
     * 更新操作
     */
    private void update(BatchExecutor batchExecutor, EtlMessage message, Map<String, Object> data) throws SQLException {
    }

    private void delete(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
    }

    /**
     * 删除操作
     */
    private void delete(BatchExecutor batchExecutor, EtlMessage message, Map<String, Object> data) throws SQLException {
    }

    private void truncate(BatchExecutor batchExecutor, EtlMessage message) throws SQLException {
    }

    /**
     * truncate操作
     */
    private void truncate(BatchExecutor batchExecutor, EtlMessage message, Map<String, Object> data) throws SQLException {
    }
}
