package com.datacenter.canal.extract;

import com.datacenter.canal.select.support.EtlMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ExtractService {

    public List<EtlMessage> extract(List<EtlMessage> messages) {
        log.debug("do extract");

        // 排除 DDL
        messages = excludeDdl(messages);

        return messages;
    }

    private List<EtlMessage> excludeDdl(List<EtlMessage> messages) {
        return messages.stream().filter(m -> !m.getIsDdl()).collect(Collectors.toList());
    }
}
