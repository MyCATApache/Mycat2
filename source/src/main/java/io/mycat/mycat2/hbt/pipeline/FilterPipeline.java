package io.mycat.mycat2.hbt.pipeline;

import java.util.function.Predicate;

public class FilterPipeline extends ReferenceHBTPipeline {
	
	Predicate predicate = null;
			
	public FilterPipeline(ReferenceHBTPipeline upStream, Predicate predicate) {
		super(upStream);
		this.predicate = predicate;
	}



}
