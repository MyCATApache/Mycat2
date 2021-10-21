package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class NormalProcedureConfig {
    String createProcedureSQL;
    NormalBackEndProcedureInfoConfig locality = new NormalBackEndProcedureInfoConfig();
}
