package io.mycat.calcite;

import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

@Getter
public class RelNodeContext {
    final RelRoot root;
    final SqlToRelConverter sqlToRelConverter;
    final SqlValidator validator;
    final RelBuilder relBuilder;
    final CalciteCatalogReader catalogReader;
    final RelDataType parameterRowType;

    public RelNodeContext(RelRoot root, SqlToRelConverter sqlToRelConverter, SqlValidator validator, RelBuilder relBuilder, CalciteCatalogReader catalogReader, RelDataType parameterRowType) {
        this.root = root;
        this.sqlToRelConverter = sqlToRelConverter;
        this.validator = validator;
        this.relBuilder = relBuilder;
        this.catalogReader = catalogReader;
        this.parameterRowType = parameterRowType;
    }
}
