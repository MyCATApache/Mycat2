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
    GlobalTableSequenceType sequenceType = GlobalTableSequenceType.NO_SEQUENCE;
    @javax.validation.constraints.NotNull
    List<GlobalBackEndTableInfoConfig> broadcast = new ArrayList<>();

    public static enum GlobalTableSequenceType{
        NO_SEQUENCE,
        GLOBAL_SEQUENCE,
        FIRST_SEQUENCE
    }
}