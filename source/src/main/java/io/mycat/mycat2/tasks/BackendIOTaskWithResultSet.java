package io.mycat.mycat2.tasks;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.MySQLPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * task处理结果集的模板类
 * <p>
 * Created by ynfeng on 2017/8/28.
 */
public abstract class BackendIOTaskWithResultSet<T extends AbstractMySQLSession> extends AbstractBackendIOTask<T> {
	
	private static Logger logger = LoggerFactory.getLogger(BackendIOTaskWithResultSet.class);
	
    protected ResultSetState curRSState = ResultSetState.RS_STATUS_COL_COUNT;

    @Override
    public void onSocketRead(T session) throws IOException {
    	
    	try {
    		if (!session.readFromChannel()){
    			return;
    		}
		}catch(IOException e){
			curRSState = ResultSetState.RS_STATUS_READ_ERROR;
			onRsFinish(session,false,e.getMessage());
			return;
		}
    	
        for (; ; ) {
           CurrPacketType currPacketType = session.resolveMySQLPackage(true);
            //因为是解析所以只处理整包
            if (currPacketType == CurrPacketType.Full) {
                MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
            	if(curMQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET && curRSState.equals(ResultSetState.RS_STATUS_COL_COUNT) ) {
    				 onRsFinish(session,false, "错误包");
            	} else {
            		switch (curRSState) {
	                    case RS_STATUS_COL_COUNT:
	                        onRsColCount(session);
	                        curRSState = ResultSetState.RS_STATUS_COL_DEF;
	                        break;
	                    case RS_STATUS_COL_DEF:
	                        if (curMQLPackgInf.pkgType == MySQLPacket.EOF_PACKET) {
	                            curRSState = ResultSetState.RS_STATUS_ROW;
	                        } else {
	                            onRsColDef(session);
	                        }
	                        break;
	                    case RS_STATUS_ROW:
	                        if (curMQLPackgInf.pkgType == MySQLPacket.EOF_PACKET) {
	                            curRSState = ResultSetState.RS_STATUS_FINISH;
	                            onRsFinish(session,true, null);
	                        } else {
	                            onRsRow(session);
	                        }
	                        break;
	                }
	            }
            } else {
                break;
            }
        }
        //设置读取过的指针
        session.proxyBuffer.readMark = session.proxyBuffer.readIndex;
    }
    
    abstract void onRsColCount(T session);

    abstract void onRsColDef(T session);

    abstract void onRsRow(T session);

    abstract void onRsFinish(T session,boolean success,String msg) throws IOException;

    public enum ResultSetState {
        /**
         * 结果集第一个包
         */
        RS_STATUS_COL_COUNT,
        /**
         * 结果集列定义
         */
        RS_STATUS_COL_DEF,
        /**
         * 结果集行数据
         */
        RS_STATUS_ROW,
        /**
         * 结果集完成
         */
        RS_STATUS_FINISH,
        
        /**
         * 结果集网络读取错误
         */
        RS_STATUS_READ_ERROR,
        
        /**
         * 结果集网络写入错误
         */
        RS_STATUS_WRITE_ERROR;
    }
}
