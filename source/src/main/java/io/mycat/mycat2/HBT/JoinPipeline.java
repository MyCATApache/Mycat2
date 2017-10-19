package io.mycat.mycat2.HBT;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.tasks.RowStream;
import io.mycat.mysql.packet.ErrorPacket;

public class JoinPipeline extends ReferenceHBTPipeline {
	

	private MycatSession mycatSession;
    private SqlMeta sqlMeta;
    private RowMeta rowMeta;
    private MatchCallback matchCallback;
    private ResultSetMeta resultSetMeta;

    private JoinMeta joinMeta;
    private List<String> joinKeyList;
    
    private int nextFetchCount = 0;
    private int nextFetchFinishCount = 0;

    private Map<String,List<List<byte[]>>> batchRow ;
    public JoinPipeline(ReferenceHBTPipeline upstream, MycatSession mycatSession, SqlMeta sqlMeta,
	        RowMeta rowMeta, JoinMeta joinMeta, ResultSetMeta resultSetMeta, MatchCallback callback) {
		super(upstream);
		this.mycatSession = mycatSession;
		this.sqlMeta = sqlMeta;
		this.rowMeta = rowMeta;
		this.resultSetMeta = resultSetMeta;
		this.matchCallback = callback;
		this.joinMeta = joinMeta;
		joinKeyList = new ArrayList<>();
		batchRow = new HashMap<>();
    
    }
    private ResultSetMeta aResultSetMeta;
    private ResultSetMeta bResultSetMeta;

	@Override
	public ResultSetMeta onHeader(ResultSetMeta header) {
		System.out.println("JoinPipeline onHeader");
		this.aResultSetMeta = header;
		
		return super.onHeader(resultSetMeta);
	}

	@Override
	public  List<byte[]> onRowData(List<byte[]> row) {
		System.out.println("JoinPipeline onRowData");
		addRow(row);
		
		row.stream().forEach(value -> {
	        System.out.print(String.format("             %s", new String(value)));
	    });
	    System.out.println("        ");
		return null;
	}
	private void addRow( List<byte[]> row) {
        Integer fieldPos = aResultSetMeta.getFieldPos(joinMeta.lJoinKey);
        String value = new String(row.get(fieldPos));
        joinKeyList.add(value);
        
        List<List<byte[]>> rows = batchRow.get(value);
        if(null == rows) {
            rows = new ArrayList<>();
            batchRow.put(value, rows);
        }
        rows.add(row);

        if(joinKeyList.size() == joinMeta.getLimit()) {
            fetchRequest();
        }
        joinKeyList.clear();
    }
	
	
	/**
    *@desc
    *@auth zhangwy @date 2017年10月17日 下午11:13:24
    **/
    private void fetchRequest() {
        System.out.println("开始fetchRequest执行 请求");
        nextFetchCount ++;
        final Map<String, List<List<byte[]>>> tmpBatchRow = batchRow;
        batchRow = new HashMap<>();
        
        Function<ResultSetMeta, ResultSetMeta> onHeader = (ResultSetMeta) ->{
            this.bResultSetMeta = ResultSetMeta;
            return null;
        } ;
        
        Function<List<byte[]>, List<byte[]>> onRowData = (bRow) -> {
            Integer rfieldPos = this.bResultSetMeta.getFieldPos(joinMeta.rJoinKey);
            String value = new String(bRow.get(rfieldPos));
            List<List<byte[]>> rows = tmpBatchRow.get(value);
            for(List<byte[]> aRow : rows) {
                List<byte[]> out = new ArrayList<>();
                this.matchCallback.call(aRow, bRow, out );
                super.onRowData(out);
            }
            return null;
        };
        MyFunction onEnd = () -> {
            this.nextFetchFinishCount ++;
            this.onFinish();
        };
        
        try {
        	Integer fieldPos = aResultSetMeta.getFieldPos(joinMeta.lJoinKey);
            int fieldType = aResultSetMeta.getFiledType(fieldPos);
            String sql = joinMeta.getSql(joinKeyList, fieldType, sqlMeta);
            System.out.println(sql);
            
            mycatSession.getBackend((mysqlsession, sender, success,result) -> {
                if(success){ 
                    
                    SqlMeta sqlMetaNew = new SqlMeta(sql, ""); 
                    RowStream rowStream = new RowStream(mysqlsession, sqlMetaNew
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
            //错误
            this.onEnd();
            e.printStackTrace();
        }
    
    }
    
    private void onFinish() {
        if(nextFetchCount == nextFetchFinishCount ) {
        	super.onEnd();
           // this.nextStream.onEnd();
        }
    }
    
    @Override
	public void onEnd() {
		System.out.println("JoinPipeline  onEnd");
		this.status = Status.SUCCESS;
		if( joinKeyList.size() != 0) {
            fetchRequest();
        } else {
            onFinish();
        }
	}

}
