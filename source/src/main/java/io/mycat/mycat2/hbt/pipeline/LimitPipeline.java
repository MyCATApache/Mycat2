package io.mycat.mycat2.hbt.pipeline;

import java.util.List;

public class LimitPipeline extends ReferenceHBTPipeline {

	private int limit;
	public LimitPipeline(ReferenceHBTPipeline upStream, 
			int limit) {
		super(upStream);
		this.limit = limit;
	}

	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		if(limit -- > 0) {
			return super.onRowData(row);
		}
		return null;
	}
}
