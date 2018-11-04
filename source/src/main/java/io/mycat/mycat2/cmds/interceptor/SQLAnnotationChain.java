package io.mycat.mycat2.cmds.interceptor;


import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLAnnotationChain {
	
	private MySQLCommand target;
	
	/**
	 * queueMap 用于去重复
	 */
	private LinkedHashMap<Long,SQLAnnotationCmd> queueMap = new LinkedHashMap<>(20);
	
	/**
	 * 前置类，后置类，around 类  动态注解  顺序，实现了SQLCommand 的动态注解会出现在此列表中
	 *  如果没有实现  SQLCommand 的 annotations 不会出现在此列表中
	 *  最终的构建结果
	 */		
	private List<SQLAnnotationCmd> queue = new ArrayList<>(20);
	
	/**
	 * queue 列表当前索引值
	 */
	private int cmdIndex = 0;
		
	/**
	 * 1. 设置原始命令
	 * @param target
	 */
	public SQLAnnotationChain setTarget(MySQLCommand target){
		this.target = target;
		return this;
	}

    /**
     * 处理路由.
     *
     * @param session
     * @return
     * @since 2.0
     */
    public SQLAnnotationChain processRoute(MycatSession session) {

        switch (session.mycatSchema.schemaType) {
            case DB_IN_ONE_SERVER:
                break;
            case DB_IN_MULTI_SERVER:
//                if (session.getCurRouteResultset() != null
//                        && session.getCurRouteResultset().getNodes().length > 1) {
//                    // DB_IN_MULTI_SERVER 模式下
//                    this.target = DbInMultiServerCmd.INSTANCE;
//                }
                break;
            case ANNOTATION_ROUTE:
                break;
//          case SQL_PARSE_ROUTE:
//              AnnotateRouteCmdStrategy.INSTANCE.matchMySqlCommand(this);
            default:
                throw new InvalidParameterException("mycatSchema type is invalid ");
        }
        return this;

    }
	
	/**
	 * 2. 处理动态注解
	 */
	public SQLAnnotationChain processDynamicAnno(MycatSession session){
//		List<SQLAnnotation> actions = new ArrayList<>(30);
//		if(AnnotationProcessor.getInstance().parse(session.sqlContext, session, actions)){
//			if(!actions.isEmpty()){
//				for(SQLAnnotation f:actions){
//					if(!f.apply(session,this)){
//						break;
//					}
//				}
//			}
//		}
		return this;
	}
	
	/**
	 * 3. 处理静态注解, 如果已经有相同的动态注解，则组装静态注解。构建步骤放在动态注解之后，可以保持动态注解的顺序
	 * @param session
	 * @param staticAnnontationMap
	 * @return
	 */
	public SQLAnnotationChain processStaticAnno(MycatSession session,Map<Byte,SQLAnnotation> staticAnnontationMap){
		BufferSQLContext context = session.sqlContext;
		byte value = context.getAnnotationType();
		SQLAnnotation staticAnno = staticAnnontationMap.get(value);
		/**
		 * 处理静态注解
		 */
		if(staticAnno!=null&&!queueMap.containsKey(staticAnno.currentKey())){
			SQLAnnotationCmd  annoCmd = staticAnno.getSqlAnnoMeta().getSQLAnnotationCmd();
			annoCmd.setSqlAnnotationChain(this);
			addCmdChain(staticAnno, annoCmd);
		}
		return this;
	}
	
	/**
	 * 4. 构建 命令 或者命令链
	 * @return
	 */
	public MySQLCommand build(){
		
		if(queueMap.isEmpty()){
			return target;
		}
		
		queue = queueMap.values().stream().collect(Collectors.toList());
		SQLAnnotationCmd annoCmd = new SQLAnnotationCmd();
		annoCmd.setSqlAnnotationChain(this);
		return annoCmd;
	}
	
	public void addCmdChain(SQLAnnotation sqlanno,SQLAnnotationCmd command){
		queueMap.put(sqlanno.currentKey(), command);
	}

	public MySQLCommand next() {
		if((queue.isEmpty())|| (cmdIndex >= queue.size())){
			cmdIndex = 0;
			return target;
		}
		return queue.get(cmdIndex++);
	}

//	@Override
//	public boolean hasNext() {
//		return (!queue.isEmpty())|| (++cmdIndex < queue.size());
//	}
}
