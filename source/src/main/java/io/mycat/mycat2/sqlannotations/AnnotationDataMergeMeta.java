package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationMergeCmd;

/**
 * Created by jamie on 2018/4/18.
 */
public class AnnotationDataMergeMeta
        implements SQLAnnotationMeta {

    @Override
    public SQLAnnotationCmd getSQLAnnotationCmd() {
        return new SQLAnnotationMergeCmd();
    }
}


