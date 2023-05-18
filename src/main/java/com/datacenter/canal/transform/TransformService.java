package com.datacenter.canal.transform;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class TransformService {

    public List<CommonMessage> transform(List<CommonMessage> messages) {
        log.debug("do transform");
        return messages;
    }
}
