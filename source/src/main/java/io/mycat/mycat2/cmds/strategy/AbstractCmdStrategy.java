package io.mycat.mycat2.cmds.strategy;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.CmdStrategy;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.mycat2.cmds.LoadDataCommand;
import io.mycat.mycat2.cmds.manager.MyCatCmdDispatcher;
import io.mycat.mycat2.sqlannotations.*;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.util.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static io.mycat.mycat2.sqlparser.BufferSQLContext.LOAD_SQL;

public abstract class AbstractCmdStrategy implements CmdStrategy {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCmdStrategy.class);

    /**
     * 进行MySQL命令的处理的容器
     */
    protected Map<Byte, MySQLCommand> MYCOMMANDMAP = new HashMap<>();

    /**
     * 进行SQL命令的处理的容器
     */
    protected Map<Byte, MySQLCommand> MYSQLCOMMANDMAP = new HashMap<>();

    private Map<Byte, SQLAnnotation> staticAnnontationMap = new HashMap<>();


    public AbstractCmdStrategy() {
        initMyCmdHandler();
        initMySqlCmdHandler();
        initStaticAnnotation();
    }

    private void initStaticAnnotation() {
        CacheResultMeta cacheResultMeta = new CacheResultMeta();
        SQLAnnotation cacheResult = new CacheResult();
        cacheResult.setSqlAnnoMeta(cacheResultMeta);
//      结果集缓存存在bug,正在重构,暂时注释掉
//		staticAnnontationMap.put(BufferSQLContext.ANNOTATION_SQL_CACHE,cacheResult);

        //hbt静态注解
        SQLAnnotation catlet = new CatletResult();
        catlet.setSqlAnnoMeta(new CatletMeta());
        staticAnnontationMap.put(BufferSQLContext.ANNOTATION_CATLET, catlet);

        AnnotationDataNode datanode = new AnnotationDataNode();
        datanode.init(new AnnotationDataNodeMeta());

        AnnotationDataMergeNode mergeNode = new AnnotationDataMergeNode();
        AnnotationDataMergeMeta dataMergeMeta = new AnnotationDataMergeMeta();
        mergeNode.init(dataMergeMeta);

        staticAnnontationMap.put(BufferSQLContext.ANNOTATION_DATANODE, datanode);
        staticAnnontationMap.put(BufferSQLContext.ANNOTATION_MERGE, mergeNode);
    }

    protected abstract void initMyCmdHandler();

    protected abstract void initMySqlCmdHandler();

    /**
     * 需要做路由的子类重写该方法.
     *
     * @param session
     * @return
     * @since 2.0
     */
    protected boolean delegateRoute(MycatSession session) {
        return true;
    }

    @Override
    public boolean matchMySqlCommand(MycatSession session) {


        MySQLCommand command = null;

        byte sqltype = 0;
        if (MySQLCommand.COM_QUERY == (byte) session.curMSQLPackgInf.pkgType) {
            /**
             * sqlparser
             */
            int rowDataIndex = session.curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize + 1;
            int length = session.curMSQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1;
            try {
                session.parser.parse(session.proxyBuffer.getBuffer(), rowDataIndex, length, session.sqlContext);
            } catch (Exception e) {
                try {
                    logger.error("sql parse error", e);
                    session.sendErrorMsg(ErrorCode.ER_PARSE_ERROR, "sql parse error : " + e.getMessage());
                } catch (Exception e1) {
                    session.close(false, e1.getMessage());
                }
                return false;
            }
            sqltype = session.sqlContext.getSQLType() != 0 ? session.sqlContext.getSQLType() : session.sqlContext.getCurSQLType();
            if (BufferSQLContext.MYCAT_SQL == sqltype) {
                session.curSQLCommand = MyCatCmdDispatcher.INSTANCE.getMycatCommand(session.sqlContext);
                return true;
            }
            command = MYSQLCOMMANDMAP.get(sqltype);
            session.setSqltype(sqltype);

        } else {
            command = MYCOMMANDMAP.get((byte) session.curMSQLPackgInf.pkgType);
            session.setSqltype((byte) session.curMSQLPackgInf.pkgType);
        }
        if (command == null) {
            command = DirectPassthrouhCmd.INSTANCE;
        }

//        if (!delegateRoute(session)) {
//            return false;
//        }

//		/**
//		 * 设置原始处理命令
//		 * 1. 设置目标命令
//		 * 2. 处理动态注解
//		 * 3. 处理静态注解
//		 * 4. 构建命令或者注解链。    如果没有注解链，直接返回目标命令
//		 */
//		SQLAnnotationChain chain = new SQLAnnotationChain();
//        session.curSQLCommand =
//                chain.setTarget(command).processDynamicAnno(session)
//                        .processStaticAnno(session, staticAnnontationMap).build();
        session.curSQLCommand = command;

        if (sqltype == LOAD_SQL){
            session.curSQLCommand = LoadDataCommand.INSTANCE;
        }

        return true;
    }
}
