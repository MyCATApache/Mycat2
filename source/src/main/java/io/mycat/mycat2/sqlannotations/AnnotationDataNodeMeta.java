package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.cmds.interceptor.SQLAnnotationCmd;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationDatanodeCmd;

/**
 * Created by jamie on 2018/4/18.
 */
public class AnnotationDataNodeMeta
        implements SQLAnnotationMeta {

    @Override
    public SQLAnnotationCmd getSQLAnnotationCmd() {
        return new SQLAnnotationDatanodeCmd();
    }
}


