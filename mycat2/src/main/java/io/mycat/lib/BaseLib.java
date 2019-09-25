package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;

public class BaseLib implements InstructionSet {
    public static Response responseOk() {
        return ConstLib.responseOk;
    }

    public static class ConstLib {
        public final static Response responseOk = (session, matcher) -> session.writeOkEndPacket();
    }
}