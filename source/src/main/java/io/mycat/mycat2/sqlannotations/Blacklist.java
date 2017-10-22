package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.ConditionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/24.
 */
public class Blacklist extends SQLAnnotation{
    List<SQLAnnotation> sqlAnnotations;
    private static final Logger logger = LoggerFactory.getLogger(Blacklist.class);

    public Blacklist() {
    	logger.debug("=>Blacklist 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        List<Map<String, String>> list = (List) args;
        sqlAnnotations = list.stream().map((i) -> getActonFactory().getActionByActionName(ConditionUtil.mappingKey(i), ConditionUtil.mappingValue(i))).collect(Collectors.toList());
        logger.debug("=>Blacklist 动态注解初始化");
    }

    @Override
    public Boolean apply(MycatSession context) {
        logger.debug("=>Blacklist");
        for (SQLAnnotation f : sqlAnnotations) {
            if (!f.apply(context)) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }


	@Override
	public MySQLCommand getMySQLCommand() {
		return null;
	}

}
