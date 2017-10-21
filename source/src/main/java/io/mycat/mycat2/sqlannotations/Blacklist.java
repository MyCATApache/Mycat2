package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;

/**
 * Created by jamie on 2017/9/24.
 */
public class Blacklist extends SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(Blacklist.class);

    public Blacklist() {
    }

    @Override
    public void init(Object args) {
    }

    @Override
    public boolean apply(MycatSession context,SQLAnnotationChain chain) {
        return true;
    }
}
