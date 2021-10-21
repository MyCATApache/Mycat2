package io.mycat.sqlhandler.procedure;

import io.mycat.config.NormalProcedureConfig;
import lombok.Getter;

@Getter
public class NormalProcedureInfo {
    int resultSetCount;
    NormalProcedureConfig config;
}
