package com.datacenter.canal.transform;

import com.datacenter.canal.select.support.EtlMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class TransformService {

    public List<EtlMessage> transform(List<EtlMessage> messages) {
        log.debug("do transform");
        return messages;
    }
}
