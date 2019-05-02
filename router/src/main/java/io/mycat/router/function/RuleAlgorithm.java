package io.mycat.router.function;

/**
 * @author mycat
 */
public interface RuleAlgorithm {

	/**
	 * init
	 * 
	 * @param
	 */
	void init();

	/**
	 * 
	 * return sharding nodes's id
	 * columnValue is column's value
	 * @return never null
	 */
	int calculate(String columnValue) ;
	
	int[] calculateRange(String beginValue,String endValue) ;
}