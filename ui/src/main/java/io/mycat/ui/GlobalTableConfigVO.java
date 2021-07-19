package io.mycat.ui;

import io.mycat.config.LogicSchemaConfig;
import lombok.Data;

@Data
public class GlobalTableConfigVO {
    String schemaName;
    String defaultTargetName;
//    String
    public static GlobalTableConfigVO from(LogicSchemaConfig config){
        GlobalTableConfigVO schemaConfigVO = new GlobalTableConfigVO();
        schemaConfigVO.schemaName = config.getSchemaName();
        schemaConfigVO.defaultTargetName = config.getTargetName();
        return schemaConfigVO;
    }

}
