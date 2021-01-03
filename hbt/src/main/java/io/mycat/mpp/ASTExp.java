package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLObject;

public interface ASTExp {

    SQLObject toParseTree();


}