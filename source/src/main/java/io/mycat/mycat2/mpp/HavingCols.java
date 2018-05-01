package io.mycat.mycat2.mpp;

import java.io.Serializable;

import io.mycat.mycat2.beans.ColumnMeta;

/**
 * Created by v1.lion on 2015/6/10.
 */
public class HavingCols implements Serializable {
	String left;
	String right;
	String operator;
	public ColumnMeta columnMeta;

	public HavingCols(String left, String right, String operator) {
		this.left = left;
		this.right = right;
		this.operator = operator;
	}

	public String getLeft() {
		return left;
	}

	public void setLeft(String left) {
		this.left = left;
	}

	public String getRight() {
		return right;
	}

	public void setRight(String right) {
		this.right = right;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public ColumnMeta getColumnMeta() {
		return columnMeta;
	}

	public void setColumnMeta(ColumnMeta columnMeta) {
		this.columnMeta = columnMeta;
	}

	
}
