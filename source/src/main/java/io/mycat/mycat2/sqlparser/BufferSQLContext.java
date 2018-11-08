package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;

import java.util.Arrays;

/**
 * <pre>
 * Created by Kaiz on 2017/3/21.
 * 2017/11/25: sqlInfo 结构调整如下：
 * 63.............................................................0
 * |------14------|---8---|---8---|-----14-----|----12----|---8---|
 *   preHashPos    realSQL SQLType   SQLSize     TBLStart  TBLCount
 * </pre>
 */
public class BufferSQLContext {
    //DDL
    public static final byte CREATE_SQL = 1;  //TODO 进一步细化。 区分 
    public static final byte ALTER_SQL = 2;   //TODO 进一步细化，区分
    public static final byte DROP_SQL = 3;    //TODO 进一步细化，区分
    public static final byte TRUNCATE_SQL = 4;
    //    public static final byte COMMENT_SQL = 5;

    // DAL (Database Administration Statements)
    public static final byte RENAME_SQL = 6;
    public static final byte USE_SQL = 7;
    public static final byte SHOW_SQL = 8;    //TODO 进一步细化。区分
    
    public static final byte SHOW_DB_SQL = 81;
    public static final byte SHOW_TB_SQL = 82;
    
    public static final byte SET_SQL = 9;     //TODO 进一步细化。区分
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
    public static final byte START_TRANSACTION_SQL = 38;
    public static final byte START_SLAVE_SQL = 39;
    public static final byte XA_START = 40;
    public static final byte XA_BEGIN = 41;
    public static final byte XA_END = 42;

    //admin command
//    public static final byte MYCAT_SWITCH_REPL = 43;
//    public static final byte MYCAT_SHOW_CONFIGS = 44;
    public static final byte MYCAT_SQL = 43;

    public static final byte SHUTDOWN_SQL = 44;

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
    public static final byte ANNOTATION_REPLICA_NAME = 10;
    public static final byte ANNOTATION_MERGE = 10;

    private short[] tblResult;  //记录格式：[{mycatSchema hash array index(defaults 0), tbl hash array index}]
    private long[] sqlInfoArray;  //用于记录sql索引，用于支持sql批量提交，格式 [{hash array start pos, sql type(15-5 hash array real sql offset, 4-0 sql type), tblResult start pos, tblResult count}]
    private byte totalTblCount;
    private int[] annotationCondition;
    private int[] selectItemArray;
    private int selectItemArrayPos;
    private int tblResultPos;
    private byte schemaCount;
    private int schemaResultPos;
    private ByteArrayInterface buffer;
    private long sqlHash;
    private byte sqlType;
    private int tblResultArraySize = 256;//todo : 测试期先写死，后期考虑从设置参数中读取
    private byte annotationType;
    private long[] annotationValue;
    private String[] annotationStringValue;
    private int totalSQLCount;
    private boolean hasLimit = false;
    private int limitStart = 0;
    private int limitCount = 0;
    private HashArray hashArray = new HashArray();
    private int curSQLIdx;
    private int curSQLTblCount = 0;
    private int preHashArrayPos = 0;
    private int preTableResultPos = 0;
    private int hashArrayRealSQLOffset = 0;//记录真实sql开始偏移
    private HashArray myCmdValue;
    private int catletNameStart = 0;
    private int catletNameLength = 0;
    private MergeAnnotation mergeAnnotation;

    public BufferSQLContext() {
        tblResult = new short[tblResultArraySize];
        sqlInfoArray = new long[256];
        annotationValue = new long[16];
        annotationStringValue = new String[16];
        annotationCondition = new int[64];
        myCmdValue = new HashArray(256);
        selectItemArray = new int[128];
        mergeAnnotation = new MergeAnnotation(this);
    }

    public void setCurBuffer(ByteArrayInterface curBuffer) {
        buffer = curBuffer;
        totalTblCount = 0;
        schemaCount = 0;
        tblResultPos = 0;
        schemaResultPos = 2;
        Arrays.fill(tblResult, (short) 0);
        Arrays.fill(sqlInfoArray, 0L);
        sqlHash = 0;
        sqlType = 0;
        annotationType = 0;
        Arrays.fill(annotationValue, 0);
        Arrays.fill(selectItemArray, 0);
        selectItemArrayPos = 0;
        hasLimit = false;
        totalSQLCount = 0;
        limitStart = 0;
        limitCount = 0;
        curSQLIdx = 0;
        curSQLTblCount = 0;
        preHashArrayPos = 0;
        preTableResultPos = 0;
        hashArrayRealSQLOffset = 0;
        myCmdValue.init();
        catletNameStart = 0;
        catletNameLength = 0;
        mergeAnnotation.clear();//@todo maybe clear in the end of parse
    }

    public void setTblName(int hashArrayPos) {
        totalTblCount++;
        curSQLTblCount++;
        tblResultPos++; //by kaiz : 跳过第一个schema，因为有可能之前已经设置过了
        tblResult[tblResultPos++] = (short) hashArrayPos;
    }

    public void pushSchemaName(int hashArrayPos) {
        schemaCount++;
        int prePos = tblResultPos - 1;
        tblResult[prePos - 1] = tblResult[prePos];
        tblResult[prePos] = (short) hashArrayPos;
    }

    public int getTableCount() {
        return totalTblCount;
    }

    public String getSchemaName(int idx) {
        int hashArrayIdx = tblResult[idx << 1];
        if (hashArrayIdx == 0)
            return "default";
        else {
            int pos = hashArray.getPos(hashArrayIdx);
            int size = hashArray.getSize(hashArrayIdx);
            return buffer.getString(pos, size);
        }
    }

    public long getTokenType(int sqlIdx, int sqlPos) {
        return hashArray.getType((int) (sqlInfoArray[sqlIdx] >>> 50) + sqlPos);
    }

    public long getTokenHash(int sqlIdx, int sqlPos) {
        return hashArray.getHash((int) (sqlInfoArray[sqlIdx] >>> 50) + sqlPos);
    }

    public long getSchemaHash(int idx) {
        int hashArrayIdx = tblResult[idx << 1];
        if (hashArrayIdx == 0)
            return 0L;
        else {
            return hashArray.getHash(hashArrayIdx);
        }
    }

    public String getTableName(int idx) {
        if (totalTblCount == 0) {
            return null;
        }
        int hashArrayIdx = tblResult[(idx << 1) + 1];
        int pos = hashArray.getPos(hashArrayIdx);
        int size = hashArray.getSize(hashArrayIdx);
        return buffer.getString(pos, size);
    }

    public String getSQLTableName(int sqlIdx, int tblIdx) {
        //int tblResultIdx = sqlInfoArray[(sqlIdx<<2)+2];
        if (sqlIdx < totalSQLCount) {
            // int sqlInfoOffset = (sqlIdx << 2) + 3;
            // int tblResultOffset = sqlInfoArray[sqlInfoOffset] >>> 8;
            // int tblResultCount = sqlInfoArray[sqlInfoOffset] & 0xFF;
            int sqlInfo = (int) sqlInfoArray[sqlIdx];
            int tblResultOffset = (sqlInfo >>> 8) & 0xFFF;
            int tblResultCount = sqlInfo & 0xFF;
            if (tblIdx < tblResultCount) {
                int hashArrayIdx = tblResult[tblResultOffset + (tblIdx << 1) + 1];
                int pos = hashArray.getPos(hashArrayIdx);
                int size = hashArray.getSize(hashArrayIdx);
                return buffer.getString(pos, size);
            } else {
                return null;
            }
        } else {
            return null;
        }

    }

    public void setSQLFinished(int curHashPos) {
        if (preHashArrayPos < curHashPos - 1) {
            int sqlSize = curHashPos - preHashArrayPos;

            totalSQLCount++;

            int idx = curSQLIdx;
            curSQLIdx++;
            long sqlInfo = ((long) preHashArrayPos & 0x3FFF) << 50;
            sqlInfo |= ((long) hashArrayRealSQLOffset & 0xFF) << 42;
            sqlInfo |= ((long) sqlType & 0xFF) << 34;
            sqlInfo |= ((long) sqlSize & 0x3FFF) << 20;
            sqlInfo |= ((long) preTableResultPos & 0xFFF) << 8;
            sqlInfo |= (long) (curSQLTblCount & 0xFF);
            sqlInfoArray[idx] = sqlInfo;
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
        if (sqlIdx < totalSQLCount) {
            return (int) sqlInfoArray[sqlIdx] & 0xFF;
        }
        return 0;
    }

    public void setSQLHash(long sqlHash) {
        this.sqlHash = sqlHash;
    }

    public long getSqlHash() {
        return this.sqlHash;
    }

    public void setSQLType(byte sqlType) {
        if (this.sqlType == 0 || this.sqlType == SELECT_SQL)
            this.sqlType = sqlType;
    }
    
    public void setShowSQLType(byte sqlType) {
        if (this.sqlType == 0 || this.sqlType == SHOW_SQL)
            this.sqlType = sqlType;
    }

    public boolean isDDL() {
        return sqlType == CREATE_SQL || sqlType == ALTER_SQL || sqlType == DROP_SQL
                || sqlType == TRUNCATE_SQL;
    }

    public boolean isSelect() {
        return this.getSQLType() == SELECT_SQL || this.getSQLType() == SELECT_INTO_SQL
                || this.getSQLType() == SELECT_FOR_UPDATE_SQL;
    }

    public void setSQLIdx(int sqlIdx) {
        curSQLIdx = sqlIdx;
    }

    public byte getSQLType() {
        return (byte) ((this.sqlInfoArray[0] >> 34) & 0xFF);
    }

    public byte getSQLType(int sqlIdx) {
        return (byte) ((this.sqlInfoArray[sqlIdx] >> 34) & 0xFF);
    }

    public byte getCurSQLType() {
        return this.sqlType;
    }

    public void setRealSQLOffset(int hashArrayPos) {
        hashArrayRealSQLOffset = hashArrayPos - preHashArrayPos;
    }

    public int getRealSQLOffset(int sqlIdx) {
        int hashArrayOffset = 0;
        if (sqlIdx <= 0) {
            hashArrayOffset = (int) (sqlInfoArray[sqlIdx] >> 42) & 0xFF;
        } else {
            hashArrayOffset = (int) (sqlInfoArray[sqlIdx] >> 50) & 0x3FFF;
        }
        return hashArray.getPos(hashArrayOffset);
    }

    public int getRealSQLSize(int sqlIdx) {
        int hashArrayEndPos = ((int) (sqlInfoArray[sqlIdx] >> 50) & 0x3FFF)
                + ((int) (sqlInfoArray[sqlIdx] >> 20) & 0x3FFF) - 1;
        if (hashArrayEndPos < 0) return 0;
        return hashArray.getPos(hashArrayEndPos) + hashArray.getSize(hashArrayEndPos);
    }

    public String getRealSQL(int sqlIdx) {
        int sqlStartPos = getRealSQLOffset(sqlIdx);
        int sqlSize = getRealSQLSize(sqlIdx) - sqlStartPos;
        return buffer.getString(sqlStartPos, sqlSize);
    }


    public void setLimit() {
        hasLimit = true;
    }

    public boolean hasLimit() {
        return this.hasLimit;
    }

    public void setLimitCount(int count) {
        limitCount = count;
    }

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
        return this.annotationType != 0;
    }

    public void setAnnotationType(byte type) {
        this.annotationType = type;
    }

    public void setAnnotationValue(byte typeKey, long value) {
        this.annotationValue[typeKey] = value;
    }

    public void setAnnotationStringValue(byte typeKey, String value) {
        this.annotationStringValue[typeKey] = value;
    }

    public String getAnnotationStringValue(byte typeKey) {
        return this.annotationStringValue[typeKey];
    }

    public void setAnnotationStart(int pos) {
    }

    public void setAnnotationSize(int size) {
    }

    public byte getAnnotationType() {
        return this.annotationType;
    }

    public long getAnnotationValue(byte typeKey) {
        return this.annotationValue[typeKey];
    }

    public HashArray getMyCmdValue() {
        return this.myCmdValue;
    }

    public String getAnnotationContent() {
        return null;
    } //by kaiz : 返回注解等号后面的内容

    public ByteArrayInterface getBuffer() {
        return buffer;
    }

    public HashArray getHashArray() {
        return hashArray;
    }

    public boolean matchDigit(int pos1, int data) {
        return TokenizerUtil.pickNumber(pos1, this.hashArray, buffer) == data;
    }

    public int matchPlaceholders(int pos1) {
        ++pos1;
//        if (hashArray.getType(pos1)== Tokenizer2.DOT){
//            ++pos1;
//            ++pos1;
//        }
        return pos1;
    }

    public int getTableIntHash(int idx) {
        int hashArrayIdx = tblResult[(idx << 1) + 1];
        int intHash = hashArray.getIntHash(hashArrayIdx);
        return intHash;
    }

    public int[] getAnnotationCondition() {
        return annotationCondition;
    }

    public byte getSchemaCount() {
        return schemaCount;
    }

    public void setSelectItem(int functionHash) {
        selectItemArray[selectItemArrayPos++] = functionHash;
    }

    public int getSelectItem(int pos) {
        if (pos > 128) {
            return 0;
        }
        return selectItemArray[pos];
    }

    public void setCatletName(int start, int length) {
        catletNameStart = start;
        catletNameLength = length;
    }

    public String getCatletName() {
        return buffer.getString(catletNameStart, catletNameLength);
    }

    public MergeAnnotation getMergeAnnotation() {
        return mergeAnnotation;
    }
}
