package io.mycat;

import io.mycat.config.NormalBackEndProcedureInfoConfig;
import io.mycat.config.NormalProcedureConfig;
import lombok.Data;

import java.util.Optional;

@Data
public class NormalProcedureHandler implements ProcedureHandler{
    private final String name;
    private final NormalProcedureConfig config;

    public NormalProcedureHandler(String name,NormalProcedureConfig config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public ProcedureType getType() {
        return ProcedureType.NORMAL;
    }

    public String getName(){
        return name;
    }
}
