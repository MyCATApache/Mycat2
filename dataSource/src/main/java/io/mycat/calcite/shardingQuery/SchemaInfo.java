package io.mycat.calcite.shardingQuery;

import lombok.*;


@EqualsAndHashCode
@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
public class SchemaInfo {
    String logicSchema;
    String logicTable;
    String targetSchema;
    String targetTable;
    String targetSchemaTable;

    public SchemaInfo(String logicSchema, String logicTable, String targetSchema, String targetTable) {
        this.logicSchema = logicSchema;
        this.logicTable = logicTable;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.targetSchemaTable = this.targetSchema + "." + this.targetTable;
    }

    public void toLowCase() {
        this.logicSchema = this.logicSchema.toLowerCase();
        this.targetTable = this.targetTable.toLowerCase();
        this.targetSchema = this.targetSchema.toLowerCase();
        this.logicTable = this.logicTable.toLowerCase();
        this.logicSchema = this.logicSchema.toLowerCase();
        this.targetSchemaTable = this.targetSchema + "." + this.targetTable;
    }

    public String getTargetSchemaTable() {
        if (this.targetSchemaTable == null){
            this.targetSchemaTable = this.targetSchema + "." + this.targetTable;
        }
        return this.targetSchemaTable ;
    }
}