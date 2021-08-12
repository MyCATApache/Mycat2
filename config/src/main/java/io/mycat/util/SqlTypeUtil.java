package io.mycat.util;

import com.alibaba.druid.sql.parser.SQLType;

public class SqlTypeUtil {
    public  static boolean isDml(SQLType  sqlType){
        switch (sqlType) {
            case ANALYZE:
            case EXPLAIN:
            case SHOW:
            case DESC:
            case SELECT:
            default:
               return false;
            case UPDATE:
            case INSERT_SELECT:
            case INSERT_VALUES:
            case INSERT:
            case DELETE:
            case MERGE:
            case CREATE:
            case ALTER:
            case DROP:
            case TRUNCATE:
            case REPLACE:
            case SET:
            case DUMP_DATA:
            case LIST:
            case WHO:
            case GRANT:
            case REVOKE:
            case COMMIT:
            case ROLLBACK:
            case USE:
            case KILL:
            case MSCK:
            case ADD_USER:
            case REMOVE_USER:
            case CREATE_USER:
            case CREATE_TABLE:
            case CREATE_TABLE_AS_SELECT:
            case CREATE_VIEW:
            case CREATE_FUNCTION:
            case CREATE_ROLE:
            case DROP_USER:
            case DROP_TABLE:
            case DROP_VIEW:
            case DROP_FUNCTION:
            case DROP_RESOURCE:
            case ALTER_USER:
            case ALTER_TABLE:
            case READ:
            case ADD_TABLE:
            case TUNNEL_DOWNLOAD:
            case UPLOAD:
            case UNKNOWN:
                return true;
        }
    }
}
