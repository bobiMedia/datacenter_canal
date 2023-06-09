package com.datacenter.canal.select;

import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.protocol.Message;
import com.datacenter.canal.process.ProcessService;
import com.datacenter.canal.select.support.EtlMessage;
import com.datacenter.canal.select.support.EtlMessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

@Slf4j
@Component
public class CanalClient implements InitializingBean {

    private final static int BATCH_SIZE = 1000;

    @Value("${canal.hostname}")
    String hostname;

    @Value("${canal.port}")
    Integer port;

    @Value("${canal.destination}")
    String destination;

    @Value("${canal.username}")
    String username;

    @Value("${canal.password}")
    String password;

    @Value("${canal.subscribe}")
    String subscribe;

    @Autowired
    ProcessService processService;

    @Override
    public void afterPropertiesSet() throws Exception {
        // 創建連接
        InetSocketAddress socketAddress = new InetSocketAddress(hostname, port);
        CanalConnector connector = CanalConnectors.newSingleConnector(socketAddress, destination, username, password);

        try {
            // 打開連接
            connector.connect();

            // 訂閱表
            if (!StringUtils.isEmpty(subscribe)) {
                connector.subscribe(subscribe);
            }

            // 回滾到未進行ack的地方，下次fetch的時候，可以從最後一個沒有ack的地方開始拿
            connector.rollback();

            while (true) {
                // 獲取指定數量的數據
                Message message = connector.getWithoutAck(BATCH_SIZE);
                // 獲取批量ID
                long batchId = message.getId();
                // 獲取批量的數量
                int size = message.getEntries().size();

                // 如果沒有數據
                if (batchId == -1 || size == 0) {
                    try {
                        // 線程休眠2秒
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        log.error("CanalConnector thread sleep error", e);
                    }
                } else {
                    // 如果有數據,則轉換資料格式
                    List<EtlMessage> messages = EtlMessageUtil.convert(message);

                    // 假如非 ddl、dml 則無法轉換，且不需處理
                    if (messages != null && !messages.isEmpty()) {
                        EtlMessage firstMsg = messages.get(0);
                        EtlMessage lastMsg = messages.get(messages.size() - 1);

                        log.info("elt start for batch: {}, range {}:{} - {}:{}", batchId, firstMsg.getLogfileName(),
                                firstMsg.getLogfileOffset(), lastMsg.getLogfileName(), lastMsg.getLogfileOffset());
                        processService.queue(messages);
                        log.info("elt end for batch: {}", batchId);
                    }
                }

                // 進行 batch id 的確認。確認之後，小於等於此 batchId 的 Message 都會被確認。
                connector.ack(batchId);
            }

        } catch (Exception e) {
            log.error("CanalConnector connect error", e);
        } finally {
            connector.disconnect();
        }
    }
}
