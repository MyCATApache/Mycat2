package io.mycat.mycat2.sqlparser.byteArrayInterface.mycat;

import java.security.InvalidParameterException;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.IntTokenHash;
import io.mycat.mycat2.sqlparser.TokenHash;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;

/**
 * Created by yanjunli on 2017/9/26.
 */
public class MYCATSQLParser {
	public static int pickMycat(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql){
		long longHash = hashArray.getHash(pos);
        if (TokenHash.SWITCH == longHash) {
            TokenizerUtil.debug(pos, context);
            return pickSwitch(++pos, arrayCount, context, hashArray, sql);
        } else if (TokenHash.SHOW == longHash) {
			TokenizerUtil.debug(pos, context);
			return pickShow(++pos, arrayCount, context, hashArray, sql);
		} else {
        	throw new InvalidParameterException(" the current mycat command is not support!!");
        }
	}
	
	private static int pickSwitch(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql){
		long longHash = hashArray.getHash(pos);
		if (TokenHash.REPL == longHash){
			context.setSQLType(BufferSQLContext.MYCAT_SWITCH_REPL);
			TokenizerUtil.debug(pos, context);
			return pickSwitchRepl(pos, arrayCount, context, hashArray, sql);
		} else {
			throw new InvalidParameterException(" the current mycat command is not support!!");
		}
	}

	private static int pickSwitchRepl(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql){
		context.getMyCmdValue().set(IntTokenHash.REPL_NAME, hashArray.getPos(++pos), hashArray.getSize(pos), hashArray.getHash(pos));
		context.getMyCmdValue().set(IntTokenHash.REPL_METABEAN_INDEX, hashArray.getPos(++pos), hashArray.getSize(pos), hashArray.getHash(pos));
		return pos;
	}

	private static int pickShow(int pos, final int arrayCount, BufferSQLContext context, HashArray hashArray, ByteArrayInterface sql){
		long longHash = hashArray.getHash(pos);
		if (TokenHash.CONFIGS == longHash){
			context.setSQLType(BufferSQLContext.MYCAT_SHOW_CONFIGS);
			TokenizerUtil.debug(pos, context);
			return pos;
		} else {
			throw new InvalidParameterException(" the current mycat command is not support!!");
		}
	}
}
