package io.mycat.util;

import java.util.Collections;
import java.util.Map;

public interface SQLContext {
    Object getSQLVariantRef(String target);

    Map<String, Object> getParameters();

    void setParameters(Map<String, Object> parameters);

    Map<String, MySQLFunction> functions();

    String getDefaultSchema();

    default void clearParameters() {
        setParameters(Collections.emptyMap());
    }

    SelectStatementHandler selectStatementHandler();

    InsertStatementHandler insertStatementHandler();

    DeleteStatementHandler deleteStatementHandler();

    LoaddataStatementHandler loaddataStatementHandler();

    SetStatementHandler setStatementHandler();

    TCLStatementHandler tclStatementHandler();

    void setDefaultSchema(String simpleName);

    UtilityStatementHandler utilityStatementHandler();
    ReplaceStatementHandler replaceStatementHandler();
    DDLStatementHandler ddlStatementHandler();

    ShowStatementHandler showStatementHandler();

    UpdateStatementHandler updateStatementHandler();

   default String simplySql(String explain){
       return explain;
   }

    long lastInsertId();

    TruncateStatementHandler truncateStatementHandler();

    HintStatementHanlder hintStatementHanlder();
}