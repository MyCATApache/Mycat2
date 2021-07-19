package io.mycat.ui;

import io.mycat.config.LogicSchemaConfig;
import javafx.fxml.FXML;
import javafx.scene.control.TextField;
import lombok.Data;


@Data
public class SchemaConfigVO {
    @FXML
    TextField schemaName;
    @FXML
    TextField defaultTargetName;
//    String
//    public static SchemaConfigVO from(LogicSchemaConfig config){
//        SchemaConfigVO schemaConfigVO = new SchemaConfigVO();
//        schemaConfigVO.schemaName = config.getSchemaName();
//        schemaConfigVO.defaultTargetName = config.getTargetName();
//        return schemaConfigVO;
//    }

}
