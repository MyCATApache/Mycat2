package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;

public interface ASTExp {

    SQLObject toParseTree();


}