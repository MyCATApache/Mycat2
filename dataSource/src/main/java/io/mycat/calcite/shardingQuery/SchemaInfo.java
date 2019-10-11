package io.mycat.calcite.shardingQuery;

import lombok.*;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
@Setter
@Builder
@ToString
public class SchemaInfo {
    String logicSchema;
    String logicTable;
    String targetSchema;
    String targetTable;

    public void toLowCase() {
        this.logicSchema = this.logicSchema.toLowerCase();
        this.targetTable = this.targetTable.toLowerCase();
        this.targetSchema = this.targetSchema.toLowerCase();
        this.logicTable = this.logicTable.toLowerCase();
        this.logicSchema = this.logicSchema.toLowerCase();
    }
}