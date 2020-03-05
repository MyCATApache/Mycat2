package io.mycat.hint;

import io.mycat.BackendTableInfo;
import io.mycat.SchemaInfo;
import io.mycat.client.Context;
import io.mycat.metadata.MetadataManager;

import java.util.Collection;
import java.util.Map;

public enum NatureValueHint implements Hint {
    INSTANCE;

    @Override
    public void accept(Context context) {
        String schemaName = context.getVariable("schemaName");
        String tableName = context.getVariable("tableName");

        if (schemaName == null) {
            Map.Entry<String, Collection<String>> next = context.getTables().entrySet().iterator().next();
            schemaName = next.getKey();
            tableName = next.getValue().iterator().next();
        }

        String equalValue = context.getVariable("equalValue");
        if (equalValue != null) {
            BackendTableInfo natrueBackEndTableInfo = MetadataManager.INSTANCE.getNatrueBackEndTableInfo(schemaName, tableName, equalValue);
            SchemaInfo schemaInfo = natrueBackEndTableInfo.getSchemaInfo();
            context.putVaribale("targets", natrueBackEndTableInfo.getTargetName());
            context.putVaribale("targetSchemaTable", schemaInfo.getTargetSchemaTable());
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public final String getName() {
        return "natureValue";
    }
}