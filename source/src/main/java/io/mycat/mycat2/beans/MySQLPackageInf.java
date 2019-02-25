package io.mycat.mycat2.beans;

/**
 * 表示MySQL 数据报文的信息
 *
 * @author wuzhihui
 * @author cjw
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

    /**
     *  mySQLPackageInf 中的判断方法,不能出现矛盾,即不能出现isFieldsCount又是ok
     * @return
     */
    public boolean isFieldsCount() {
        return ((this.pkgLength == 5 || this.pkgLength == 7 || this.pkgLength == 8 || this.pkgLength == 13) && this.pkgType != 0) && !isOkPacket();
    }

    public boolean isERRPacket() {
        return (this.pkgType == 0xff);
    }

    public boolean isOkPacket() {
        return (this.pkgType == 0 && this.pkgLength > 7) || (this.pkgType == 0xfe && !isEOFPacket());
    }

    public boolean notOkEofErrPacket() {
        return !isOkPacket() && !isEOFPacket() && !isERRPacket();
    }

    public boolean isCompletionPacket() {
        return isOkPacket() || isEOFPacket();
    }

    public boolean maybePrepareOkPacket() {
        return (this.pkgType == 0 && this.pkgLength == 12);
    }

    public boolean maybeColumnDefinition41() {
        return (this.pkgType == 'd' && this.pkgLength > 21);
    }

    public boolean isLocalInfileRequest() {
        return (this.pkgType == 0xfb && this.pkgLength > 6);
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
