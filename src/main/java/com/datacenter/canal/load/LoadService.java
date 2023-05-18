package com.datacenter.canal.load;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class LoadService {

    public void load(List<CommonMessage> messages) {
        log.debug("do load");
    }
}
