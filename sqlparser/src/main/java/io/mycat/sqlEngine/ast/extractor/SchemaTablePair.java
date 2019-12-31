package io.mycat.sqlEngine.ast.extractor;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class SchemaTablePair {
    String schemaName;
    String tableName;
}