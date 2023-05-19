package com.datacenter.canal.process;

import com.datacenter.canal.extract.ExtractService;
import com.datacenter.canal.load.LoadService;
import com.datacenter.canal.select.support.EtlMessage;
import com.datacenter.canal.transform.TransformService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Slf4j
@Service
public class ProcessService {

    @Autowired
    ExtractService extractService;

    @Autowired
    TransformService transformService;

    @Autowired
    LoadService loadService;

    public void queue(List<EtlMessage> messages) throws SQLException {
        // TODO 目前直接執行 ETL，預定改為 Queue 管理
        log.debug("do etl process");
        messages = extractService.extract(messages);

        if(!messages.isEmpty()) {
            messages = transformService.transform(messages);
        }
        
        if(!messages.isEmpty()) {
            try {
                loadService.load(messages);
            } catch (SQLException e) {
                log.error("do load process failed: {}", e.getMessage());
            }
        }
        
    }
}
