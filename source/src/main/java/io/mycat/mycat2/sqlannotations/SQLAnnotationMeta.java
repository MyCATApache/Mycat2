package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;

/**
 * sql 注解元数据
 * @author yanjunli
 *
 */
public interface SQLAnnotationMeta {
	SQLAnnotationCmd getSQLAnnotationCmd();
}
