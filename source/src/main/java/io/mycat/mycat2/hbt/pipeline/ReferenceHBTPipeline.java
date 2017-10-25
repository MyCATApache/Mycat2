package io.mycat.mycat2.hbt.pipeline;

import java.util.List;
import java.util.function.Function;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.hbt.JoinMeta;
import io.mycat.mycat2.hbt.MatchCallback;
import io.mycat.mycat2.hbt.OrderMeta;
import io.mycat.mycat2.hbt.GroupPairKey;
import io.mycat.mycat2.hbt.GroupPairKeyMeta;
import io.mycat.mycat2.hbt.ResultSetMeta;
import io.mycat.mycat2.hbt.RowMeta;
import io.mycat.mycat2.hbt.SqlMeta;

public class ReferenceHBTPipeline implements AbstractHBTPipeline,OpPipeline{
	
	public static enum Status {
		RUNNING, SUCCESS, ERROR
	};
	
	Status status = Status.RUNNING;
	private ReferenceHBTPipeline upStream = null;
	private ReferenceHBTPipeline nextStream = null;
	
	
	/*把流串起来*/
	ReferenceHBTPipeline(ReferenceHBTPipeline upStream) {
		this.upStream = upStream;
		if(this.upStream != null) {
			this.upStream.nextStream = this;
		}
	}
	
	/*判断是否是头结点*/
	boolean isHeaderPipeline() {
		return upStream == null;
	}
	
	protected boolean canHandle() {
		return Status.RUNNING.equals(status) || Status.SUCCESS.equals(status);
	}
	
	@Override
	public ResultSetMeta onHeader(ResultSetMeta header) {
		if(canHandle()) {
			try{
				return this.nextStream.onHeader(header);
			} catch (Exception e) {
				onError(e);
			}

		}
		return null;
	}

	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		if(canHandle()) {
			try{
				return this.nextStream.onRowData(row);
			} catch (Exception e) {
				onError(e);
			}
		}
		return null;
	}

	@Override
	public void onEnd() {
		if(canHandle()) {
			try{
				this.nextStream.onEnd();
			} catch (Exception e) {
				onError(e);
			}
		}
	}
	@Override
	public void onError(Throwable throwable) {
		this.status = Status.ERROR;
		this.nextStream.onError(throwable);
	}

    /* 
     * 
     */
    @Override
    public  OpPipeline group(
    		GroupPairKeyMeta keyFunction,  ResultSetMeta resultSetMeta,
    		List<Function<List<List<byte[]>>,List<byte[]>>> opFunction) {
    	 return new GroupByPipeline(this, keyFunction, resultSetMeta
    			 ,opFunction);
    }

    /* 
     * 
     */
    @Override
    public  OpPipeline limit(int limit) {
        return new LimitPipeline(this, limit);
    };
	@Override
	public OpPipeline skip(int n) {
        return new SkipPipeline(this, n);
	}
    public OpPipeline join(MycatSession session, SqlMeta sqlMeta,
            RowMeta rowMeta, JoinMeta joinMeta, ResultSetMeta resultSetMeta, MatchCallback callback)  {
        return new JoinPipeline(this,  session,  sqlMeta,
                rowMeta,  joinMeta, resultSetMeta, callback);
    }

    /* 
     * @see io.mycat.mycat2.HBT.OpPipeline#filter()
     */
    @Override
    public OpPipeline filter() {
        return null;
    }

    /* 
     * @see io.mycat.mycat2.HBT.OpPipeline#out(io.mycat.mycat2.MycatSession)
     */
    @Override
    public void out(MycatSession session) {
        ReferenceHBTPipeline header = new OutPipeline(this, session);
          while(header.upStream != null) {
              header = header.upStream;
          }
          header.begin(0);
    }

	@Override
	public OpPipeline order(OrderMeta orderMeta) {
		
		 return new OrderPipeline(this, orderMeta);
	}




}
