package io.mycat.proxy;

import java.nio.ByteBuffer;

public class StringUtil {
	  public final static String dumpAsHex(final ByteBuffer g, final int offset, final int length) {
	        final StringBuilder out = new StringBuilder(length * 4);
	        final int end = offset + length;
	        int p    = offset;
	        int rows = length / 8;

	        // rows
	        for (int i = 0; (i < rows) && (p < end); i++) {
	            // - hex string in a line
	            for (int j = 0, k = p; j < 8; j++, k++) {
	                final String hexs = Integer.toHexString(g.get(k) & 0xff);
	                if (hexs.length() == 1) {
	                	out.append('0');
	                }
	                out.append(hexs).append(' ');
	            }
	            out.append("    ");
	            // - ascii char in a line
	            for (int j = 0; j < 8; j++, p++) {
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
	        //LOGGER.debug("offset = {}, length = {}, end = {}, n = {}", offset, length, end, n);
	        // padding hex string in line
	        for (int i = n; i < 8; i++) {
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
	    
	    
}
