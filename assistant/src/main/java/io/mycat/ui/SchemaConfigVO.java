package io.mycat.ui;

import javafx.fxml.FXML;
import javafx.scene.control.TextField;
import lombok.Data;


@Data
public class SchemaConfigVO implements VO{
    @FXML
    TextField schemaName;
    @FXML
    TextField defaultTargetName;

    @Override
    public String toJsonConfig() {
        return null;
    }
//    String
//    public static SchemaConfigVO from(LogicSchemaConfig config){
//        SchemaConfigVO schemaConfigVO = new SchemaConfigVO();
//        schemaConfigVO.schemaName = config.getSchemaName();
//        schemaConfigVO.defaultTargetName = config.getTargetName();
//        return schemaConfigVO;
//    }

}
