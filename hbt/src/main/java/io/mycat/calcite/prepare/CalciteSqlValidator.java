package io.mycat.calcite.prepare;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/** Validator. */
class CalciteSqlValidator extends SqlValidatorImpl {

  CalciteSqlValidator(SqlOperatorTable opTab,
                      CalciteCatalogReader catalogReader, JavaTypeFactory typeFactory,
                      Config config) {
    super(opTab, catalogReader, typeFactory, config);
  }

  @Override protected RelDataType getLogicalSourceRowType(
      RelDataType sourceRowType, SqlInsert insert) {
    final RelDataType superType =
        super.getLogicalSourceRowType(sourceRowType, insert);
    return ((JavaTypeFactory) typeFactory).toSql(superType);
  }

  @Override protected RelDataType getLogicalTargetRowType(
      RelDataType targetRowType, SqlInsert insert) {
    final RelDataType superType =
        super.getLogicalTargetRowType(targetRowType, insert);
    return ((JavaTypeFactory) typeFactory).toSql(superType);
  }
}
