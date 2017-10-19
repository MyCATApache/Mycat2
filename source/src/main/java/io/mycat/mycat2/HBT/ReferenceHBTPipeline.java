package io.mycat.mycat2.HBT;

import java.util.List;

import io.mycat.mycat2.MycatSession;

public   class ReferenceHBTPipeline implements AbstractHBTPipeline,OpPipeline{
	
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
			return this.nextStream.onHeader(header);
		}
		return null;
	}

	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		if(canHandle()) {
			return this.nextStream.onRowData(row);
		}
		return null;
	}

	@Override
	public void onEnd() {
		if(canHandle()) {
			this.nextStream.onEnd();
		}
	}
	@Override
	public void onError(String msg) {
		this.status = Status.ERROR;
		this.nextStream.onError(msg);
	}

    /* 
     * 
     */
    @Override
    public  OpPipeline group() {
        return null;
    }

    /* 
     * 
     */
    @Override
    public  OpPipeline limit() {
        return null;
    };
    
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


}
