package io.mycat.mycat2.HBT;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.function.Function;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.tasks.RowStream;
import io.mycat.mysql.packet.ErrorPacket;

public class HeaderHBTPipeline extends ReferenceHBTPipeline {
	
    MycatSession mycatSession = null;
    
	SqlMeta sqlMeta = null;

	private RowMeta rowData;
	HeaderHBTPipeline(MycatSession mycatSession, SqlMeta sqlMeta, RowMeta rowData) {
		super(null);
		this.sqlMeta = sqlMeta;
		this.rowData = rowData;
		this.mycatSession = mycatSession;
	}
	
	@Override
	public void begin(int i)   {
		System.out.println("开始header执行");
		Function<ResultSetMeta, ResultSetMeta> onHeader = this::onHeader;
		Function<List<byte[]>, List<byte[]>> onRowData = this::onRowData;
		MyFunction onEnd = this::onEnd;
		
		try {
            mycatSession.getBackend((mysqlsession, sender, success,result) -> {
                if(success){ 
                    RowStream rowStream = new RowStream(mysqlsession, sqlMeta 
                            ,onHeader, onRowData, onEnd);
                    rowStream.fetchStream();
                    rowStream.setCallback((optSession, sender1, success1,result1) -> {
                        if(success) {
                           //结果回调
                            onEnd.apply();
                        }else{
                            MycatSession session = optSession.getMycatSession();
                            session.proxyBuffer.flip();
                            session.takeOwner(SelectionKey.OP_WRITE);
                            session.responseOKOrError((ErrorPacket)result1);
                        }
                    });
                } else {
                    //错误
                    onEnd.apply();
                }		    
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

//	@Override
//	public ResultSetMeta onHeader(ResultSetMeta header)  {
//		return this.nextStream.onHeader(header);
//	}
//
//	@Override
//	public List<byte[]> onRowData(List<byte[]> row)  {
//		return this.nextStream.onRowData(row);
//
//	}
//
//	@Override
//	public void onEnd()  {
//	    System.out.println("================ headerHBTPIpeline onEnd ================");
//		this.nextStream.onEnd();
//	}

 

}
