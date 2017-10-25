package io.mycat.mycat2.HBT;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.proxy.ProxyBuffer;

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
	        System.out.print(String.format("             %s", new String(value)));
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
			mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_HBT_TABLE_META.getKey(), tableMeta);
		}
		try {
			mycatSession.writeToChannel();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		System.out.println("outPipeline onEnd");
	}

}
