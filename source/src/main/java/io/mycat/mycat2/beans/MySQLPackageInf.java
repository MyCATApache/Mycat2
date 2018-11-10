package io.mycat.mycat2.beans;

/**
 * 表示MySQL 数据报文的信息
 *
 * @author wuzhihui
 */
public class MySQLPackageInf {
    public int pkgType;
    public boolean crossBuffer;
    public int startPos;
    public int endPos;
    public int pkgLength;
    /**
     * 还有多少字节才结束，仅对跨多个Buffer的MySQL报文有意义（crossBuffer=true)
     */
    public int remainsBytes;

    public boolean isFieldsCount() {
        return (this.pkgLength <= 7&&this.pkgType!=0) && !isOkPacket();
    }
    public boolean isERRPacket() {
        return (this.pkgType == 0xff);
    }

    public boolean isOkPacket() {
        return (this.pkgType == 0 && this.pkgLength > 7) || (this.pkgType == 0xfe && !isEOFPacket());
    }

    public boolean isEOFPacket() {
        return (this.pkgType == 0xfe) && this.pkgLength < 9;
    }

    @Override
    public String toString() {
        return "MySQLPackageInf [pkgType=" + pkgType + ", crossBuffer=" + crossBuffer + ", startPos=" + startPos
                + ", endPos=" + endPos + ", pkgLength=" + pkgLength + ", remainsBytes=" + remainsBytes + "]";
    }
}
