package io.mycat.mycat2.common;

import java.util.concurrent.ExecutorService;

/**
 * @author wuzh
 */
public interface NameableExecutorService extends ExecutorService {

	public String getName();
}
