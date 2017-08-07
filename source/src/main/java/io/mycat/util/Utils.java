package io.mycat.util;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

public class Utils {
	private static final String illegalChars = "/" + '\u0000' + '\u0001' + "-" + '\u001F' + '\u007F' + "-" + '\u009F' + '\uD800' + "-" + '\uF8FF' + '\uFFF0'
			+ "-" + '\uFFFF';
	private static final Pattern p = Pattern.compile("(^\\.{1,2}$)|[" + illegalChars + "]");

	public static void validateFolder(String name) {
		if (name == null || name.length() == 0) {
			throw new IllegalArgumentException("folder name is emtpy");
		}
		if(name.length() > 255) {
			throw new IllegalArgumentException("folder name is too long");
		}
		if (p.matcher(name).find()) {
			throw new IllegalArgumentException("folder name [" + name + "] is illegal");
		}
	}

	public static boolean isFilenameValid(String file) {
		File f = new File(file);
		try {
			f.getCanonicalPath();
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	
    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        File[] subs = dir.listFiles();
        if (subs != null) {
            for (File f : dir.listFiles()) {
                if (f.isFile()) {
                    if(!f.delete()) {
                        throw new IllegalStateException("delete file failed: "+f);
                    }
                } else {
                    deleteDirectory(f);
                }
            }
        }
        if(!dir.delete()) {
            throw new IllegalStateException("delete directory failed: "+dir);
        }
    }

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();

	public static String randomString(int len )
	{
		StringBuilder sb = new StringBuilder( len );
		for( int i = 0; i < len; i++ )
			sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
		return sb.toString();
	}


	public static void deleteFile(File file) {
    	if (!file.exists() || !file.isFile()) {
    		return;
    	}
    	if (!file.delete()) {
            throw new IllegalStateException("delete file failed: "+file);
    	}
    }
}
