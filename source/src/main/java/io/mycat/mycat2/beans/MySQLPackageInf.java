package io.mycat.mycat2.beans;
/**
 *表示MySQL 数据报文的信息
 * @author wuzhihui
 *
 */
public class MySQLPackageInf {
public byte pkgType;
public boolean crossBuffer;
public int startPos;
public int endPos;
public int pkgLength;
/**
 * 还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
 */
public int remainsBytes;
}
