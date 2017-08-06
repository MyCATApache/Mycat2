package io.mycat.mycat2;
/**
 *表示MySQL 数据报文的信息
 * @author wuzhihui
 *
 */
public class MySQLPackageInf {
public int pkgType;
public boolean crossBuffer;
public int remainsBytes;
public int startPos;
public int length;
}
