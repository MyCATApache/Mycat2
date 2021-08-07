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
    @javax.validation.constraints.NotNull
    List<GlobalBackEndTableInfoConfig> broadcast = new ArrayList<>();
}