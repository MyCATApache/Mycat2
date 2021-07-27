package io.mycat.config;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
@Builder
public class GlobalTableConfig {
    String createTableSQL;
    String balance;
    List<GlobalBackEndTableInfoConfig> broadcast = new ArrayList<>();
}