package io.mycat.mycat2.hbt.pipeline;

import java.util.List;

public class SkipPipeline extends ReferenceHBTPipeline {

	private int n;
	public SkipPipeline(ReferenceHBTPipeline upStream, 
			int n) {
		super(upStream);
		this.n = n;
	}

	@Override
	public List<byte[]> onRowData(List<byte[]> row) {
		if(n -- > 0) {
			return null;
		}
		return super.onRowData(row);
	}
}
