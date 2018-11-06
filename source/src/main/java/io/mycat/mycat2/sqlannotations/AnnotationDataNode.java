package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jamie on 2018/4/17.
 */
public class AnnotationDataNode extends SQLAnnotation {

    private static final Logger logger = LoggerFactory.getLogger(AnnotationDataNode.class);

    /**
     * 动态注解 组装 mysqlCommand chain
     */
    @Override
    public boolean apply(MycatSession session, SQLAnnotationChain chain) {
        return true;
    }

    @Override
    public void init(Object args) {
        setSqlAnnoMeta((SQLAnnotationMeta) args);
    }
}
