package io.mycat.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.mycat.mycat2.AbstractMySQLSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * String utils.
 * 
 * @author little-pan
 * @since 2016-09-29
 *
 */
public final class StringUtil {
	
	final static Logger LOGGER = LoggerFactory.getLogger(StringUtil.class);
	
	private StringUtil(){
		
	}
	
	/**
	 * An interface that gets a byte from buffer such as byte array etc.
	 * 
	 * @author little-pan
	 * @since 2016-09-26
	 * 
	 */
	public interface ByteGetable{
		
		byte get(int i);
		
	}
	
	public static class ByteArrayGetable implements ByteGetable{
		final byte[] buffer;
		
		public ByteArrayGetable(final byte[] buffer){
			this.buffer = buffer;
		}

		@Override
		public byte get(final int i) {
			return (buffer[i]);
		}
		
	}
	
	
	public static class ByteBufferGetable implements ByteGetable{
		
		final ByteBuffer buffer;
		
		public ByteBufferGetable(final ByteBuffer buffer){
			this.buffer = buffer;
		}

		@Override
		public byte get(final int i){
			return (buffer.get(i));
		}
		
	}
	
	public final static String dumpAsHex(final byte[] buffer) {
		return dumpAsHex(buffer, 0, buffer.length);
	}
	
	public final static String dumpAsHex(final byte[] buffer, final int length) {
		return dumpAsHex(buffer, 0, length);
	}
	
	public final static String dumpAsHex(final byte[] buffer, final int offset, final int length) {
		return dumpAsHex(new ByteArrayGetable(buffer), offset, length);
	}
	
	/**
     * Dumps the given bytes as a hex dump (from offset up to length bytes).
     * 
     * @param byteBuffer the data to print as hex
     * @param offset the begin index of bytes to print
     * @param length the number of bytes to print
     * @param g the get a byte interface from buffer such as byte array etc
     * 
     * @return hex string
     */
    public final static String dumpAsHex(final ByteGetable g, final int offset, final int length) {
        final StringBuilder out = new StringBuilder(length * 4);
        final int end = offset + length;
        int p    = offset;
        int wide = 32;
        int rows = length / wide;

        // rows
        for (int i = 0; (i < rows) && (p < end); i++) {
            // - hex string in a line
            for (int j = 0, k = p; j < wide; j++, k++) {
                final String hexs = Integer.toHexString(g.get(k) & 0xff);
                if (hexs.length() == 1) {
                	out.append('0');
                }
                out.append(hexs).append(' ');
            }
            out.append("    ");
            // - ascii char in a line
            for (int j = 0; j < wide; j++, p++) {
                final int b = 0xff & g.get(p);
                if (b > 32 && b < 127) {
                	out.append((char) b);
                } else {
                	out.append('.');
                }
                out.append(' ');
            }
            out.append('\n');
        }

        // remain bytes
        int n = 0;
        for (int i = p; i < end; i++, n++) {
            final String hexs = Integer.toHexString(g.get(i) & 0xff);
            if (hexs.length() == 1) {
            	out.append('0');
            }
            out.append(hexs).append(' ');
        }
        LOGGER.debug("offset = {}, length = {}, end = {}, n = {}", offset, length, end, n);
        // padding hex string in line
        for (int i = n; i < wide; i++) {
        	out.append("   ");
        }
        out.append("    ");
        
        for (int i = p; i < end; i++) {
            final int b = 0xff & g.get(i);
            if (b > 32 && b < 127) {
            	out.append((char) b);
            } else {
            	out.append('.');
            }
            out.append(' ');
        }
        if(p < end){
        	out.append('\n');
        }
        
        return (out.toString());
    }
    
    public final static String dumpAsHex(final ByteBuffer buffer){
    	return (dumpAsHex(buffer, 0, buffer.position()));
    }
    
    public final static String dumpAsHex(final ByteBuffer buffer, final int length){
    	return (dumpAsHex(buffer, 0, length));
    }
    
    public final static String dumpAsHex(final ByteBuffer buffer, final int offset, final int length){
    	return (dumpAsHex(new ByteBufferGetable(buffer), offset, length));
    }
	public final static String dumpMySQLPackageInfAsHex(AbstractMySQLSession mySQLSession){
		return (dumpAsHex(new ByteBufferGetable(mySQLSession.proxyBuffer.getBuffer()), mySQLSession.curMSQLPackgInf.startPos, mySQLSession.curMSQLPackgInf.endPos));
	}
    public final static boolean isEmpty(String str) {
    	return str == null || str == "";
    }
    public final static String parseString(byte[] bytes) {
    	if(null != bytes) {
    		return new String(bytes);
    	}
    	return null ;
    }
//    public final static String dumpAsHex(final ConDataBuffer buffer){
//    	return (dumpAsHex(buffer, 0, buffer.getWritePos()));
//    }
//    
//    public final static String dumpAsHex(final ConDataBuffer buffer, final int length){
//    	return (dumpAsHex(buffer, 0, length));
//    }
//    
//    public final static String dumpAsHex(final ConDataBuffer buffer, final int offset, final int length){
//    	return (dumpAsHex(new ConDataBufferGetable(buffer), offset, length));
//    }
    
    /**
	 * 移除`符号
	 * @param str
	 * @return
	 */
	public static String removeBackquote(String str){
		//删除名字中的`tablename`和'value'
		if (str.length() > 0) {
			StringBuilder sb = new StringBuilder(str);
			if (sb.charAt(0) == '`'||sb.charAt(0) == '\'') {
				sb.deleteCharAt(0);
			}
			if (sb.charAt(sb.length() - 1) == '`'||sb.charAt(sb.length() - 1) == '\'') {
				sb.deleteCharAt(sb.length() - 1);
			}
			return sb.toString();
		}
		return "";
	}
    
    public static void main(String args[]){
    	final byte[] array = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 48, 49, 50, 97, 98, 99};
    	
    	System.out.println("test - byte array");
    	System.out.println(dumpAsHex(array, 0));
    	System.out.println(dumpAsHex(array, 0, 5));
    	System.out.println(dumpAsHex(array, 8));
    	System.out.println(dumpAsHex(array, 15));
    	System.out.println(dumpAsHex(array));
    	
    	System.out.println("test - ByteBuffer");
    	final ByteBuffer buffer = ByteBuffer.wrap(array);
    	buffer.position(buffer.limit());
    	System.out.println(dumpAsHex(buffer));
    }    
}
