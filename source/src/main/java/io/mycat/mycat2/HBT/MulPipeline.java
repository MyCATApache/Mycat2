package io.mycat.mycat2.HBT;

import java.util.ArrayList;
import java.util.List;

import io.mycat.mycat2.MycatSession;

public class MulPipeline   extends ReferenceHBTPipeline  {
	
	MycatSession mycatSession = null;
	private int nextFetchCount = 0;
    private int nextFetchFinishCount = 0;
    private List<ReferenceHBTPipeline> pipeList ;
    
	MulPipeline(ReferenceHBTPipeline upStream, MycatSession mycatSession) {
	
		super(upStream);
		this.mycatSession = mycatSession;
		pipeList = new ArrayList<>();
	
	}
    
    public void addPipeline(SqlMeta sqlMeta, ReferenceHBTPipeline pipeLine) {
    	
    	pipeList.add(pipeLine);
    	pipeLine.begin(0);
    	
    	
    	
    }
    
    
    @Override
    public void onEnd() {
    	if(nextFetchCount == nextFetchFinishCount ) {
    		
        	super.onEnd();
    	}
    	
    }
    
}
