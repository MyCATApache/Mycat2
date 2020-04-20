package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLAssignItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSetStatement;
import io.mycat.meta.MetadataService;
import io.mycat.meta.MetadataService.BackendMetadata;
import io.mycat.meta.MetadataService.FrontendMetadata;
import io.mycat.meta.MetadataService.ProxyMetadata;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.util.Receiver;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * set语法的实现
 *
 * case1: 修改系统变量(只对新客户端生效)
 *   set global 变量名:=值;
 *   set @@gloabl.变量名:=值;
 *
 * case2: 修改会话变量
 *   set @变量名:=值;
 *
 * case3: 修改系统变量(只对当前客户端生效)
 *   set 变量名:=值;
 *
 * case4: 查询并修改会话变量
 *   select @变量名:=select 字段 from 表;
 *
 * case5: 查询
 *   select @@autocommit,@@auto_increment_increment;
 *   show variables like 'auto_increment_increment';
 *
 * @author wangzihaogithub 2020年4月19日 21:18:11
 */
@Resource
public class SetSQLHandler extends AbstractSQLHandler<SQLSetStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLSetStatement> request, Receiver response, MycatSession session) {
        int sessionId = session.sessionId();
        SQLSetStatement ast = request.getAst();
        List<SQLAssignItem> unknowItemList = new ArrayList<>();
        List<SQLAssignItem> items = ast.getItems();
        for (SQLAssignItem item : items) {
            boolean isSetValue = onSQLAssignItem(item, session,request,response);
            if(!isSetValue) {
                unknowItemList.add(item);
            }
        }
        if(!unknowItemList.isEmpty()) {
            onUnknowMetadata(unknowItemList);
        }
        return items.size() == unknowItemList.size()? CODE_0 : CODE_200;
//        RootHelper.INSTANCE.getConfigProvider().currentConfig().get
    }

    private boolean onSQLAssignItem(SQLAssignItem item,MycatSession session,SQLRequest<SQLSetStatement> request,Receiver receiver) {
        int sessionId = session.sessionId();
        SQLExpr key = item.getTarget();
        SQLExpr value = item.getValue();
        String metadataKey = key.toString();
        String metadataValue = value.toString();
        boolean isGlobal;
        boolean isSession;
        if(key instanceof SQLVariantRefExpr){
            SQLVariantRefExpr sqlVariantRefExpr = ((SQLVariantRefExpr) key);
            isGlobal = sqlVariantRefExpr.isGlobal();
            isSession = sqlVariantRefExpr.isSession();
            //如果不是全局也不是会话. 就是case3: 修改系统变量(只对当前客户端生效) set 变量名:=值;
            if(!isGlobal && !isSession){
                isSession = true;
            }
        }else {
            isGlobal = false;
            isSession = true;
        }

        boolean isSetValue = false;

        //设置前端的元数据
        if(isFrontendMetadata(metadataKey,isGlobal,isSession)){
            if(isGlobal){
                FrontendMetadata metadata = mycatMetadataService.getFrontendMetadata();
                metadata.setValue(metadataKey,metadataValue);
                isSetValue = true;
            }
            if(isSession){
                FrontendMetadata metadata = mycatMetadataService.getFrontendMetadata(sessionId);
                metadata.setValue(metadataKey,metadataValue);
                isSetValue = true;
            }
        }

        //设置后端的元数据
        if(isBackendMetadata(metadataKey,isGlobal,isSession)){
            if(isGlobal){
                BackendMetadata metadata = mycatMetadataService.getBackendMetadata();
                metadata.setValue(metadataKey,metadataValue);
                isSetValue = true;
            }
            if(isSession){
                BackendMetadata metadata = mycatMetadataService.getBackendMetadata(sessionId);
                metadata.setValue(metadataKey,metadataValue);

                //怎么广播给后端所有实例
                String sql = "SET " + item;
//                session.getCommandHandler().handleQuery(sql.getBytes(),session);
//                for (MySQLClientSession mySQLClientSession : session.getMySQLSession().getSessionManager().getAllSessions()) {
//                    mySQLClientSession.writeProxyBufferToChannel(sql.getBytes());
//                }
//                session.getDataContext().setVariable();
//                receiver.proxyUpdate("",sql);
                receiver.sendOk();
                isSetValue = true;
            }
        }

        //设置代理的元数据
        if(isProxyMetadata(metadataKey,isGlobal,isSession)){
            if(isGlobal){
                ProxyMetadata metadata = mycatMetadataService.getProxyMetadata();
                metadata.setValue(metadataKey,metadataValue);
                isSetValue = true;
            }
            if(isSession){
                ProxyMetadata metadata = mycatMetadataService.getProxyMetadata(sessionId);
                metadata.setValue(metadataKey,metadataValue);
                isSetValue = true;
            }
        }
        return isSetValue;
    }

    private void onUnknowMetadata(List<SQLAssignItem> unknowItemList){

    }

    private boolean isBackendMetadata(String metadataKey,boolean isGlobal,boolean isSession){
        return true;
    }

    private boolean isFrontendMetadata(String metadataKey,boolean isGlobal,boolean isSession){
        return false;
    }

    private boolean isProxyMetadata(String metadataKey,boolean isGlobal,boolean isSession){
        return true;
    }

}
