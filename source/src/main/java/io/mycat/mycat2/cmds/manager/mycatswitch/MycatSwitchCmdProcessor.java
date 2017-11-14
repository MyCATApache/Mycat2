package io.mycat.mycat2.cmds.manager.mycatswitch;

import java.util.ArrayList;
import java.util.List;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.manager.MycatCmdHolder;
import io.mycat.mycat2.cmds.manager.MycatCmdProcssor;
import io.mycat.mycat2.cmds.manager.NotSupportCmdHolder;
import io.mycat.mycat2.cmds.manager.ParseContext;

/**
 * mycat switch 命令
 * @author yanjunli
 *
 */
public class MycatSwitchCmdProcessor implements MycatCmdProcssor {
	
	private static class LazyHolder {    
	     private static final MycatSwitchCmdProcessor INSTANCE = new MycatSwitchCmdProcessor();    
	}
	
	public static final MycatSwitchCmdProcessor getInstance() {    
       return LazyHolder.INSTANCE;    
   } 
	
	private static List<MycatCmdHolder> cmds = new ArrayList<>();
	
	static{
		cmds.add(MycatSwitchReplCmd.INSTANCE);
	}

	@Override
	public MySQLCommand getCommand(ParseContext context) {
		return cmds.stream()
				   .filter(f->f.apply(context))
				   .findFirst()
				   .orElse(NotSupportCmdHolder.INSTANCE);
	}

	/**
	 * 检查 switch
	 */
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
                return switchSCheck(t,i);
            default:
                return Boolean.FALSE;
            }
        }
		return Boolean.FALSE;
	}
	
	private Boolean switchSCheck(ParseContext t,int offset){
    	if (t.sql.length() > offset + "WITCH".length()) {
            char c1 = t.sql.charAt(++offset);
            char c2 = t.sql.charAt(++offset);
            char c3 = t.sql.charAt(++offset);
            char c4 = t.sql.charAt(++offset);
            char c5 = t.sql.charAt(++offset);
            if ((c1 == 'W' || c1 == 'w') && (c2 == 'I' || c2 == 'i') && (c3 == 'T' || c3 == 't')
            		&& (c4 == 'C' || c4 == 'c')&& (c5 == 'H' || c5 == 'h')) {
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
