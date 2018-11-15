package io.mycat.mycat2.hbt.pipeline;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;

import io.mycat.mycat2.CurSQLState;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.hbt.ResultSetMeta;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ErrorCode;

public class OutPipeline extends ReferenceHBTPipeline {
	
	private MycatSession mycatSession;
	
	private TableMeta tableMeta = new TableMeta();
	
	public OutPipeline(ReferenceHBTPipeline upstream, MycatSession mycatSession) {
		super(upstream);
		this.mycatSession = mycatSession;
	}

	@Override
	public ResultSetMeta onHeader(ResultSetMeta header) {
		System.out.println("outPipeline onHeader");
		tableMeta.init(header);
		return null;
	}

	@Override
	public  List<byte[]> onRowData(List<byte[]> row) {
	    row.stream().forEach(value -> {
	    	if(null != value) {
				System.out.print(String.format("             %s", new String(value)));
			} else {
				System.out.print(String.format("             null"));
			}
	    });
	    System.out.println("        ");
	    tableMeta.addFieldValues(row);
	    return null;
	}

	@Override
	public void onEnd() {
		
		ProxyBuffer buffer = mycatSession.proxyBuffer;
		buffer.reset();
		tableMeta.writeBegin(buffer);
		tableMeta.writeRowData(buffer);
		buffer.flip();
		buffer.readIndex = buffer.writeIndex; 
		mycatSession.takeOwner(SelectionKey.OP_WRITE);
		if(!tableMeta.isWriteFinish()) {
            mycatSession.curSQLSate.set(CurSQLState.HBT_TABLE_META, tableMeta);
		}
		try {
			mycatSession.writeToChannel();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		System.out.println("outPipeline onEnd");
	}
	
	@Override
	public void onError(Throwable throwable) {
		String msg = throwable.getMessage();
		try {
			mycatSession.takeBufferOwnerOnly();
			mycatSession.sendErrorMsg(ErrorCode.ERR_FOUND_EXCEPION, msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
