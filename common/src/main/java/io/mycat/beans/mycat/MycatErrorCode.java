/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mycat;

import io.mycat.MycatException;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.HashMap;

/**
 * #444
 */
public class MycatErrorCode {

//    init:
//    MYCAT-3002:ERR_FETCH_METADATA
//    MYCAT-3036:ERR_INIT_CONFIG

    public static int ERR_FETCH_METADATA = 3002;
    public static int ERR_INIT_CONFIG = 3036;

    //    common:
//    MYCAT-3029:ERR_QUERY_CANCLED
//    MYCAT-3034:ERR_NOT_SUPPORT
    public static int ERR_QUERY_CANCELED = 3029;
    public static int ERR_NOT_SUPPORT = 3034;

    //    query:
//    MYCAT-3001:ERR_TABLE_NOT_EXIST
//    MYCAT-3010:ERR_STATEMENT_PARAMS
//    MYCAT-3012:ERR_SQL_QUERY_TIMEOUT
//    MYCAT-3013:ERR_SQL_DISTRIBUTED_QUERY_TIMEOUT
//    MYCAT-3014:ERR_PARSE
//    MYCAT-3021:ERR_MODIFY_SHARDING_COLUMN
//    MYCAT-3022:ERR_SHARDING_KEY_NOT_EXIST
//    MYCAT-3023:ERR_INSERT_SHARDING_KEY_NOT_EXIST
//    MYCAT-3026:ERR_MODIFY_SYSTEM_TABLE
    public static int ERR_TABLE_NOT_EXIST = 3001;
    public static int ERR_STATEMENT_PARAMS = 3010;
    public static int ERR_SQL_QUERY_TIMEOUT = 3012;
    public static int ERR_SQL_DISTRIBUTED_QUERY_TIMEOUT = 3013;
    public static int ERR_PARSE = 3014;
    public static int ERR_MODIFY_SHARDING_COLUMN = 3021;
    public static int ERR_SHARDING_KEY_NOT_EXIST = 3022;
    public static int ERR_INSERT_SHARDING_KEY_NOT_EXIST = 3023;
    public static int ERR_MODIFY_SYSTEM_TABLE = 3026;


    //    optimizer:
//    MYCAT-3019:ERR_OPTIMIZE
//    MYCAT-3020:ERR_OPTIMIZE_HINT
    public static int ERR_OPTIMIZE = 3019;
    public static int ERR_OPTIMIZE_HINT = 3020;

    //    cluster:
//    MYCAT-3003:ERR_CLUSTER_NOT_AVALILABLE
//    MYCAT-3009:ERR_DB_DOWN
//    MYCAT-3011:ERR_CLUSTER_NOT_STABLE
    public static int ERR_CLUSTER_NOT_AVALILABLE = 3003;
    public static int ERR_DB_DOWN = 3009;
    public static int ERR_CLUSTER_NOT_STABLE = 3011;


//    connection:
//    MYCAT-3004:ERR_GET_CONNECTION_UNKNOWN_REASON
//    MYCAT-3005:ERR_GET_CONNECTION_KNOWN_REASON
//    MYCAT-3006:ERR_GET_CONNECTION_POOL_FULL
//    MYCAT-3008:ERR_CONNECTION_ACCESS_DENIED
//    MYCAT-3024:ERR_CONNECTION_CHARSET
//    MYCAT-3030:ERR_UNKNOWN_DATABASE
//    MYCAT-3035:ERR_CONNECTION_CLOSED

    public static int ERR_GET_CONNECTION_UNKNOWN_REASON = 3004;
    public static int ERR_GET_CONNECTION_KNOWN_REASON = 3005;
    public static int ERR_GET_CONNECTION_POOL_FULL = 3006;
    public static int ERR_CONNECTION_ACCESS_DENIED = 3008;
    public static int ERR_CONNECTION_CHARSET = 3024;
    public static int ERR_UNKNOWN_DATABASE = 3030;
    public static int ERR_CONNECTION_CLOSED = 3035;

//    sequence:
//    MYCAT-3014:ERR_SEQUENCE_NEXT_VALUE
//    MYCAT-3015:ERR_SEQUENCE_NOT_EXIST
//    MYCAT-3016:ERR_SEQUENCE_TABLE_NOT_EXIST
//    MYCAT-3017:ERR_SEQUENCE_TABLE_SCHEMA
//    MYCAT-3018:ERR_INIT_SEQUENCE

    public static int ERR_SEQUENCE_NEXT_VALUE = 3014;
    public static int ERR_SEQUENCE_NOT_EXIST = 3015;
    public static int ERR_SEQUENCE_TABLE_NOT_EXIST = 3016;
    public static int ERR_SEQUENCE_TABLE_SCHEMA = 3017;
    public static int ERR_INIT_SEQUENCE = 3018;

    //    executer:
//    MYCAT-3026:ERR_EXECUTOR
//    MYCAT-3025:ERR_CAST
//    MYCAT-3027:ERR_FUNCTION
//
    public static int ERR_EXECUTOR = 3026;
    public static int ERR_CAST = 3025;
    public static int ERR_FUNCTION = 3027;


    //    transcation:
//    MYCAT-3028:ERR_TRANS
//    MYCAT-3030:ERR_TRANS_PARAM
//    MYCAT-3031:ERR_TRANS_TERMINATED
//    MYCAT-3032:ERR_TRANS_FINISHED
//    MYCAT-3033:ERR_TRANS_UNSUPPORTED
    public static int ERR_TRANS = 3028;
    public static int ERR_TRANS_PARAM = 3030;
    public static int ERR_TRANS_TERMINATED = 3031;
    public static int ERR_TRANS_FINISHED = 3032;
    public static int ERR_TRANS_UNSUPPORTED = 3033;

    private static final HashMap<Integer, String> map = new HashMap<>();

    static {
        for (Field field : MycatErrorCode.class.getFields()) {
            try {
                int errorCode = field.getInt(null);
                map.put(errorCode,field.getName());
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

    }

    public static String errorCode(int errorCode) {
        String domain = map.get(errorCode);
        return MessageFormat.format("ERROR-CODE: [MYCAT-{0}][{1}] ",errorCode+"",domain);
    }

    public static void main(String[] args) {

        String s = errorCode(ERR_TRANS_TERMINATED);
        System.out.println();
    }

    public static MycatException createMycatException(int errorCode,String message,Throwable throwable){
        if (map.containsKey(errorCode)){
            return new MycatException(errorCode, errorCode(errorCode)+message,throwable);
        }
         return new MycatException(errorCode, message,throwable);
    }
    public static MycatException createMycatException(int errorCode,String message){
        if (map.containsKey(errorCode)){
            return new MycatException(errorCode, errorCode(errorCode)+message);
        }
        return new MycatException(errorCode, message);
    }
}
