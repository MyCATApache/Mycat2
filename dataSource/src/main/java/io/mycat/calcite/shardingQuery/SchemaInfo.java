package io.mycat.calcite.shardingQuery;

import lombok.*;

import java.util.Objects;


@EqualsAndHashCode
@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
public class SchemaInfo {
    final String targetSchema;
    final String targetTable;
    final String targetSchemaTable;

    public SchemaInfo(String targetSchema, String targetTable) {
        this.targetSchema = Objects.requireNonNull(targetSchema).toLowerCase();
        this.targetTable = Objects.requireNonNull(targetTable).toLowerCase();
        this.targetSchemaTable = this.targetSchema + "." + this.targetTable;
    }

}