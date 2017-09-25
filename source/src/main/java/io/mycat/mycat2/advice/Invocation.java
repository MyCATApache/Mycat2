package io.mycat.mycat2.advice;

import io.mycat.mycat2.MySQLCommand;

public interface Invocation extends MySQLCommand {
	
	public void setCommand(MySQLCommand command);
}
