package io.mycat.mycat2.cmds.manager;

import java.util.function.Function;

import io.mycat.mycat2.MySQLCommand;

public interface MycatCmdHolder extends MySQLCommand,Function<ParseContext,Boolean> {

}
