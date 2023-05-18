package com.datacenter.canal.extract;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ExtractService {

    public List<CommonMessage> extract(List<CommonMessage> messages) {
        log.debug("do extract");
        return messages;
    }
}
