package com.datacenter.canal.transform;

import com.datacenter.canal.select.support.EtlColumn;
import com.datacenter.canal.select.support.EtlMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class TransformService {

    public List<EtlMessage> transform(List<EtlMessage> messages) {
        log.debug("do transform");
        Set<String> columnMaskedSet = new HashSet<>();
        columnMaskedSet.add("password");
        messages.stream()
                .flatMap(message -> Stream.concat(message.getData().stream(), message.getOld().stream()))
                .flatMap(map -> map.values().stream())
                .map(column -> {
                    if (columnMaskedSet.contains(column.getName())) {
                        column.setValue(columnValMasked("*", column.getValue().toString().length()));
                    }
                    return column;
                })
                .collect(Collectors.toList());
        return messages;
    }

    private static String columnValMasked(String str, int count){
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            stringBuilder.append(str);
        }

        return stringBuilder.toString();
    }
}
