package com.datacenter.canal.process;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.datacenter.canal.extract.ExtractService;
import com.datacenter.canal.load.LoadService;
import com.datacenter.canal.transform.TransformService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

    public void queue(List<CommonMessage> messages) {
        // TODO 目前直接執行 ETL，預定改為 Queue 管理
        log.debug("do etl process");
        messages = extractService.extract(messages);
        messages = transformService.transform(messages);
        loadService.load(messages);
    }
}
