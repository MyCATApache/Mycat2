package io.mycat.proxy.buffer;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.beans.conf.ProxyBean;
import io.mycat.mycat2.beans.conf.ProxyConfig;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;

/**
 * bufferpool 分配器
 * @author yanjunli
 *
 */
public class BufferPooLFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(BufferPooLFactory.class);
	
    public final  static double DIRECT_SAFETY_FRACTION  = 0.7;
	
	private ProxyBean proxybean;
	
	private List<BufferPool> bufferPools = new ArrayList<>();
	
	private static class LazyHolder {    
	     private static final BufferPooLFactory INSTANCE = new BufferPooLFactory();    
	}
	
	public static final BufferPooLFactory getInstance() {    
      return LazyHolder.INSTANCE;    
    }
		
	public BufferPooLFactory(){
		
		int poolCount = ProxyRuntime.INSTANCE.getNioReactorThreads();
		
		ProxyConfig proxyConfig = ProxyRuntime.INSTANCE.getConfig().getConfig(ConfigEnum.PROXY);
		
		proxybean = proxyConfig.getProxy();
		
		long directMemorySize = (long)(Platform.getMaxDirectMemory()*DIRECT_SAFETY_FRACTION);
		
		int expectPoolSize = Double.valueOf(directMemorySize / poolCount).intValue();
		
		short bufferPoolPageNumber = proxybean.getBufferPoolPageNumber();
		
		int bufferPoolPageSize = proxybean.getBufferPoolPageSize();
		
		int poolsize = bufferPoolPageNumber * bufferPoolPageSize;
		
		if(expectPoolSize < poolsize){
			logger.warn("max directMemory is {}",directMemorySize);
			logger.warn("bufferPoolPageNumber * bufferPoolPageSize > expectPoolSize. "
					+ "bufferPoolPageNumber is {},bufferPoolPageSize is {},expectPoolSize is {} "
					+ "please check it!!!!",bufferPoolPageNumber,bufferPoolPageSize,expectPoolSize);
			
			logger.warn("try in bufferPool settings ");
			if(directMemorySize < bufferPoolPageSize * poolCount ){
				throw new InvalidParameterException("directMemorySize  is too small!!!. min {"+bufferPoolPageSize * poolCount+"}");
			}
			
			bufferPoolPageNumber = Double.valueOf(expectPoolSize / bufferPoolPageSize).shortValue();
			logger.info("in bufferPoolPageNumber to {}",bufferPoolPageNumber);
			proxybean.setBufferPoolPageNumber(bufferPoolPageNumber);
		}
		
		IntStream.range(0, poolCount).forEach(f->{
			bufferPools.add(new DirectByteBufferPool(proxybean.getBufferPoolPageSize(),
					proxybean.getBufferPoolChunkSize(),
					proxybean.getBufferPoolPageNumber()));
		});
	}
	
	/**
	 * @return
	 */
	public BufferPool getBufferPool(){
		if(bufferPools.size()==0){
			throw new InvalidParameterException("bufferPool is empty. maybe a bug,please fix it!!!!!");
		}
		return bufferPools.remove(bufferPools.size()-1);
	}
	
	public void recycle(BufferPool pool){
		bufferPools.add(pool);
	}
}
