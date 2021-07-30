package io.mycat.ui;

import io.mycat.config.LogicSchemaConfig;
import io.vertx.core.json.Json;
import javafx.fxml.FXML;
import javafx.scene.control.TextField;
import lombok.Data;
import org.jetbrains.annotations.NotNull;


@Data
public class SchemaConfigVO implements VO{
    @FXML
    TextField schemaName;
    @FXML
    TextField defaultTargetName;

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getLogicSchemaConfig());
    }

    @NotNull
    private LogicSchemaConfig getLogicSchemaConfig() {
        String schemaName = getSchemaName().getText();
        String targetName = getDefaultTargetName().getText();
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        logicSchemaConfig.setSchemaName(schemaName);
        logicSchemaConfig.setTargetName(targetName);
        return logicSchemaConfig;
    }
//    String
//    public static SchemaConfigVO from(LogicSchemaConfig config){
//        SchemaConfigVO schemaConfigVO = new SchemaConfigVO();
//        schemaConfigVO.schemaName = config.getSchemaName();
//        schemaConfigVO.defaultTargetName = config.getTargetName();
//        return schemaConfigVO;
//    }

}
