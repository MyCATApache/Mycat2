package io.mycat.mycat2.cmds.manager.show;

import java.util.ArrayList;
import java.util.List;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.manager.MycatCmdHolder;
import io.mycat.mycat2.cmds.manager.MycatCmdProcssor;
import io.mycat.mycat2.cmds.manager.NotSupportCmdHolder;
import io.mycat.mycat2.cmds.manager.ParseContext;

/**
 *  mycat show 命令
 * @author yanjunli
 *
 */
public class MycatShowCmdProcessor implements MycatCmdProcssor {
	
	private static class LazyHolder {    
	     private static final MycatShowCmdProcessor INSTANCE = new MycatShowCmdProcessor();    
	}
	
	public static final MycatShowCmdProcessor getInstance() {    
        return LazyHolder.INSTANCE;    
    }
	
	private static List<MycatCmdHolder> cmds = new ArrayList<>();
	
	//新命令在这里注册
	static{
		cmds.add(MycatShowConfigsCmd.INSTANCE);
	}

	@Override
	public MySQLCommand getCommand(ParseContext context) {
		return cmds.stream()
				   .filter(f->f.apply(context))
				   .findFirst()
				   .orElse(NotSupportCmdHolder.INSTANCE);
	}

	@Override
	public Boolean apply(ParseContext t) {
		int i = t.offset;
		int length = t.sql.length();
        for (; i < length; i++) {
            switch (t.sql.charAt(i)) {
            case ' ':
                continue;
            case 'S':
            case 's':
                return showSCheck(t,i);
            default:
                return Boolean.FALSE;
            }
        }
		return null;
	}
	
	private Boolean showSCheck(ParseContext t,int offset){
    	if (t.sql.length() > offset + "HOW".length()) {
            char c1 = t.sql.charAt(++offset);
            char c2 = t.sql.charAt(++offset);
            char c3 = t.sql.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'O' || c2 == 'o') && (c3 == 'W' || c3 == 'w')) {
                if (t.sql.length() > ++offset && t.sql.charAt(offset) != ' ') {
                    return Boolean.FALSE;
                }
                t.offset = offset;
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }
}
