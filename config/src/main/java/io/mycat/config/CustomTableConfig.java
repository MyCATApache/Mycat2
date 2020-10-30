package io.mycat.config;

import lombok.*;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
@Builder
public class CustomTableConfig {
    String createTableSQL;
    String clazz;
    Map<String,Object> kvOptions;
    List<Object> listOptions;
}