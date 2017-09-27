package io.mycat.mycat2.beans;

public class CheckResult {
	
	private boolean success;
	private String msg;
	
	public CheckResult(){}
	
	public CheckResult(boolean success){
		this.success = success;
	}
	
	public CheckResult(boolean success,String msg){
		this.success = success;
		this.msg = msg;
	}

	public String getMsg() {
		return msg;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}
}
