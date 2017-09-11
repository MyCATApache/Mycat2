package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;

import java.util.Arrays;

/**
 * Created by Fanfan on 2017/3/21.
 */
public class NewSQLContext {
    //DDL
    public static final byte CREATE_SQL = 1;
    public static final byte ALTER_SQL = 2;
    public static final byte DROP_SQL = 3;
    public static final byte TRUNCATE_SQL = 4;
    //    public static final byte COMMENT_SQL = 5;
    public static final byte RENAME_SQL = 6;
    public static final byte USE_SQL = 7;
    public static final byte SHOW_SQL = 8;
    public static final byte SET_SQL = 9;
    public static final byte PARTITION_SQL = 10;

    //DML
    public static final byte SELECT_SQL = 11;
    public static final byte UPDATE_SQL = 12;
    public static final byte DELETE_SQL = 13;
    public static final byte INSERT_SQL = 14;
    public static final byte REPLACE_SQL = 15;
    public static final byte CALL_SQL = 16;
    public static final byte EXPLAIN_SQL = 17;

    public static final byte DESCRIBE_SQL = 18;
    public static final byte HANDLER_SQL = 19;
    public static final byte LOAD_SQL = 20;
    public static final byte HELP_SQL = 21;

    //DCL
    public static final byte GRANT_SQL = 22;
    public static final byte REVOKE_SQL = 23;
    public static final byte KILL_SQL = 24;
    public static final byte KILL_QUERY_SQL = 25;

    //TCL
    public static final byte START_SQL = 26;
    public static final byte BEGIN_SQL = 27;

    public static final byte TRANSACTION_SQL = 28;
    public static final byte SAVEPOINT_SQL = 29;
    public static final byte ROLLBACK_SQL = 30;
    public static final byte SET_TRANSACTION_SQL = 31;
    public static final byte LOCK_SQL = 32;
    public static final byte XA_SQL = 33;
    public static final byte SET_AUTOCOMMIT_SQL = 34;
    public static final byte COMMIT_SQL = 35;
//    public static final byte COMMIT_SQL = 17;
    public static final byte SELECT_INTO_SQL = 36;
    public static final byte SELECT_FOR_UPDATE_SQL = 37;

    //ANNOTATION TYPE
    public static final byte ANNOTATION_BALANCE = 1;
    public static final byte ANNOTATION_SQL = 2;
    public static final byte ANNOTATION_DB_TYPE = 3;
    public static final byte ANNOTATION_SCHEMA = 4;
    public static final byte ANNOTATION_DATANODE = 5;
    public static final byte ANNOTATION_CATLET = 6;
    public static final byte ANNOTATION_SQL_CACHE = 7;
    public static final byte ANNOTATION_ACCESS_COUNT = 7;
    public static final byte ANNOTATION_AUTO_REFRESH = 8;
    public static final byte ANNOTATION_CACHE_TIME = 9;

    private short[] tblResult;  //记录格式：[{schema hash array index(defaults 0), tbl hash array index}]
    private short[] sqlInfoArray;  //用于记录sql索引，用于支持sql批量提交，格式 [{hash array start pos, sql type(15-5 hash array real sql offset, 4-0 sql type), tblResult start pos, tblResult count}]
    private byte totalTblCount;
    private int tblResultPos;
    private byte schemaCount;
    private int schemaResultPos;
    private byte[] buffer;
    private long sqlHash;
    private byte sqlType;
    private int tblResultArraySize = 256;//todo : 测试期先写死，后期考虑从设置参数中读取
    private byte annotationType;
    private long[] annotationValue;
    private int totalSQLCount;
    private boolean hasLimit = false;
    private int limitStart = 0;
    private int limitCount = 0;
    private HashArray hashArray;
    private int curSQLIdx;
    private int curSQLTblCount = 0;
    private int preHashArrayPos = 0;
    private int preTableResultPos = 0;
    private int hashArrayRealSQLOffset = 0;//记录真实sql开始偏移

    public NewSQLContext() {
        tblResult = new short[tblResultArraySize];
        sqlInfoArray = new short[512];
        annotationValue = new long[16];
    }

    public void setCurBuffer(byte[] curBuffer, HashArray hashArray) {
        buffer = curBuffer;
        this.hashArray = hashArray;
        totalTblCount = 0;
        schemaCount = 0;
        tblResultPos = 0;
        schemaResultPos = 2;
        Arrays.fill(tblResult, (short)0);
        Arrays.fill(sqlInfoArray, (short)0);
        sqlHash = 0;
        sqlType = 0;
        annotationType = 0;
        Arrays.fill(annotationValue, 0);
        hasLimit = false;
        totalSQLCount = 0;
        limitStart = 0;
        limitCount = 0;
        curSQLIdx = 0;
        curSQLTblCount = 0;
        preHashArrayPos = 0;
        preTableResultPos = 0;
        hashArrayRealSQLOffset = 0;
    }

    public void setTblName(int hashArrayPos) {
        totalTblCount++;
        curSQLTblCount++;
        tblResultPos++; //by kaiz : 跳过第一个schema，因为有可能之前已经设置过了
        tblResult[tblResultPos++] = (short)hashArrayPos;
    }

    public void pushSchemaName(int hashArrayPos) {
        schemaCount++;
        int prePos = tblResultPos-1;
        tblResult[prePos-1] = tblResult[prePos];
        tblResult[prePos] = (short)hashArrayPos;
    }

    public int getTableCount() { return totalTblCount; }

    public String getSchemaName(int idx) {
        int hashArrayIdx = tblResult[idx<<1];
        if (hashArrayIdx == 0)
            return "default";
        else {
            int pos = hashArray.getPos(hashArrayIdx);
            int size = hashArray.getSize(hashArrayIdx);
            return new String(buffer, pos, size);
        }
    }

    public long getSchemaHash(int idx) {
        int hashArrayIdx = tblResult[idx<<1];
        if (hashArrayIdx == 0)
            return 0L;
        else {
            return hashArray.getHash(hashArrayIdx);
        }
    }

    //todo : 测试期返回String，将来应该要返回hashcode
    public String getTableName(int idx) {
        int hashArrayIdx = tblResult[(idx<<1)+1];
        int pos = hashArray.getPos(hashArrayIdx);
        int size = hashArray.getSize(hashArrayIdx);
        return new String(buffer, pos, size);
    }

    public String getSQLTableName(int sqlIdx, int tblIdx) {
        //int tblResultIdx = sqlInfoArray[(sqlIdx<<2)+2];
        if (sqlIdx < totalSQLCount) {
            int sqlInfoOffset = (sqlIdx<<2)+3;
            int tblResultOffset = sqlInfoArray[sqlInfoOffset] >>> 8;
            int tblResultCount = sqlInfoArray[sqlInfoOffset] & 0xFF;
            if (tblIdx < tblResultCount) {
                int hashArrayIdx = tblResult[tblResultOffset+(tblIdx<<1)+1];
                int pos = hashArray.getPos(hashArrayIdx);
                int size = hashArray.getSize(hashArrayIdx);
                return new String(buffer, pos, size);
            } else {
                return null;
            }
        } else {
            return null;
        }

    }

    public void setSQLFinished(int curHashPos) {
        if (preHashArrayPos < curHashPos-1) {
            int sqlSize = curHashPos - preHashArrayPos;

            totalSQLCount++;

            int idx = curSQLIdx<<2;
            curSQLIdx++;
            sqlInfoArray[idx++] = (short)preHashArrayPos;
            sqlInfoArray[idx++] = (short)((hashArrayRealSQLOffset<<6) | sqlType);
            sqlInfoArray[idx++] = (short)sqlSize;
            sqlInfoArray[idx] = (short)((preTableResultPos<<8) | curSQLTblCount);
            curSQLTblCount = 0;
            preTableResultPos = tblResultPos;
            preHashArrayPos = curHashPos;
            sqlType = 0;
        } else {
            //all sql has been parsed
            curSQLIdx = 0;//rewind index to 0;
        }
    }

    public int getSQLCount() {
        return totalSQLCount;
    }

    public int getSQLTblCount(int sqlIdx) {
        if (sqlIdx< totalSQLCount) {
            return sqlInfoArray[(sqlIdx<<2)+3] & 0xFF;
        }
        return 0;
    }

    public void setSQLHash(long sqlHash) { this.sqlHash = sqlHash; }

    public long getSqlHash() { return this.sqlHash; }

    public void setSQLType(byte sqlType) {
        if (this.sqlType == 0 || this.sqlType == SELECT_SQL)
            this.sqlType = sqlType;
    }

    public void setSQLIdx(int sqlIdx) {
        curSQLIdx = sqlIdx;
    }

    public byte getSQLType() { return (byte)(this.sqlInfoArray[1] & 0x3F); }
    public byte getSQLType(int sqlIdx) { return (byte)(this.sqlInfoArray[(sqlIdx<<2)+1] & 0x3F); }
    public byte getCurSQLType() { return this.sqlType; }

    public void setRealSQLOffset(int hashArrayPos) {
        hashArrayRealSQLOffset = hashArrayPos - preHashArrayPos;
    }
    public int getRealSQLOffset(int sqlIdx) {
        int hashArrayOffset = sqlInfoArray[(sqlIdx<<2)+1] >>> 6;
        return hashArray.getPos(hashArrayOffset);
    }
    public int getRealSQLSize(int sqlIdx) {
        int hashArrayEndPos = sqlInfoArray[(sqlIdx<<2)+2]-1;
        return hashArray.getPos(hashArrayEndPos)+hashArray.getSize(hashArrayEndPos);
    }
    public String getRealSQL(int sqlIdx) {
        int sqlStartPos = getRealSQLOffset(sqlIdx);
        int sqlSize =getRealSQLSize(sqlIdx) - sqlStartPos;
        return new String(buffer, sqlStartPos, sqlSize);
    }


    public void setLimit() { hasLimit = true; }
    public boolean hasLimit() { return this.hasLimit; }
    public void setLimitCount(int count) { limitCount = count; }
    public void pushLimitStart() {
        limitStart = limitCount;
    }
    public void setLimitStart(int start) {
        limitStart = start;
    }
    public int getLimitStart() {
        return limitStart;
    }
    public int getLimitCount() {
        return limitCount;
    }

    public boolean hasAnnotation() { //by kaiz : 是否包含注解，此处还需要完善
        return this.annotationType!=0;
    }

    public void setAnnotationType(byte type) {
        this.annotationType = type;
    }
    public void setAnnotationValue(byte typeKey, long value) { this.annotationValue[typeKey] = value; }
    public void setAnnotationStart(int pos) {}
    public void setAnnotationSize(int size) {}
    public byte getAnnotationType() { return this.annotationType; }
    public long getAnnotationValue(byte typeKey) { return this.annotationValue[typeKey]; }
    public String getAnnotationContent() { return null; } //by kaiz : 返回注解等号后面的内容

}
