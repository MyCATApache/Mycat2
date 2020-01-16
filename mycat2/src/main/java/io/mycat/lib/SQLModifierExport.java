package io.mycat.lib;

import io.mycat.pattern.InstructionSet;

import java.text.MessageFormat;
/**
 * @author chen junwen
 */
public class SQLModifierExport implements InstructionSet {
    public final static String messageFormat(String pattern,Object ... arguments) {
        return MessageFormat.format(pattern,arguments);
    }

}