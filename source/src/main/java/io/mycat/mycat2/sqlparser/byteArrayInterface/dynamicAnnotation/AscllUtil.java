package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

/**
 * Created by troshin@foxmail.com on 2017/8/17.
 */
public class AscllUtil {
   public static String shiftAscll(String str, boolean isNull) {
        switch (str) {
            case "!":
                return "EXCLAMATION";
            case "\"":
                return "DOUBLEQUOTE";
            case "#":
                return "COMMENTS";
            case "$":
                return "DOLLAR";
            case "%":
                return "PERCENT";
            case "\'":
                return "CLOSEQUOTES";
            case "`":
                return "ORDERQUOTES";
            case "(":
                return "LEFTBRACKET";
            case ")":
                return "RIGHTBRACKET";
            case "*":
                return "ASTERISK";
            case "+":
                return "PLUS";
            case ",":
                return "COMMA";
            case "-":
                return "MINUS";
            case ".":
                return "DOT";
            case "/":
                return "FORWARDSLASH";
            case "\\":
                return "BACKSLASH";
            case ":":
                return "COLON";
            case ";":
                return "SEMICOLON";
            case "<":
                return "LESSTHAN";
            case "=":
                return "EQUAL";
            case ">":
                return "GREATERTHAN";
            case "?":
                return "QUESTIONMARK";
            case "@":
                return "AT";
            case "{":
                return "OPENBRACE";
            case "}":
                return "CLOSEBRACE";
            case "|":
                return "VERTICAL";
            case "~":
                return "BREAKLINE";
            case "[":
                return "LEFTOPENBRACKET";
            case "]":
                return "RIGHTCLOASBRACKET";
            case "^":
                return "CARET";
            case "&":
                return "AMPERSAND";
            case "_":
                return "UNDERLINE";


            case "0":
                return "ZERO";
            case "1":
                return "ONE";
            case "2":
                return "TWO";
            case "3":
                return "TREE";
            case "4":
                return "FOUR";
            case "5":
                return "FIVE";
            case "6":
                return "SIX";
            case "7":
                return "SEVEN";
            case "8":
                return "EIGHT";
            case "9":
                return "NINE";

            case "a":
                return "a";
            case "b":
                return "b";
            case "c":
                return "c";
            case "d":
                return "d";
            case "e":
                return "e";
            case "f":
                return "f";
            case "g":
                return "g";
            case "h":
                return "h";
            case "i":
                return "i";
            case "j":
                return "j";
            case "k":
                return "k";
            case "l":
                return "l";
            case "m":
                return "m";
            case "n":
                return "n";
            case "o":
                return "o";
            case "p":
                return "p";
            case "q":
                return "q";
            case "r":
                return "r";
            case "s":
                return "s";
            case "t":
                return "t";
            case "u":
                return "u";
            case "v":
                return "v";
            case "w":
                return "w";
            case "x":
                return "x";
            case "y":
                return "y";
            case "z":
                return "z";

            case "A":
                return "A";
            case "B":
                return "B";
            case "C":
                return "C";
            case "D":
                return "D";
            case "E":
                return "E";
            case "F":
                return "F";
            case "G":
                return "G";
            case "H":
                return "H";
            case "I":
                return "I";
            case "J":
                return "J";
            case "K":
                return "K";
            case "L":
                return "L";
            case "M":
                return "M";
            case "N":
                return "N";
            case "O":
                return "O";
            case "P":
                return "P";
            case "Q":
                return "Q";
            case "R":
                return "R";
            case "S":
                return "S";
            case "T":
                return "T";
            case "U":
                return "U";
            case "V":
                return "V";
            case "W":
                return "W";
            case "X":
                return "X";
            case "Y":
                return "Y";
            case "Z":
                return "Z";

            case "":
                return "NUL";                   //空字符
            case "SOH":
                return "SOH";                //标题开始
            case "STX":
                return "STX";                //正文开始
            case "ETX":
                return "ETX";                //正文结束
            case "EOT":
                return "EOT";                //传输结束
            case "ENQ":
                return "ENQ";                //请求
            case "ACK":
                return "ACK";                //收到通知
            case "BEL":
                return "BEL";                //响铃
            case "BS":
                return "BS";                  //退格
            case "HT":
                return "HT";                  //水平制表符
            case "LF":
                return "LF";                  //换行键
            case "VT":
                return "VT";                  //垂直制表符
            case "FF":
                return "FF";                  //换页键
            case "CR":
                return "CR";                  //回车键
            case "SO":
                return "SO";                  //不用切换
            case "SI":
                return "SI";                  //启动切换
            case "DLE":
                return "DLE";                //数据链路转义
            case "DC1":
                return "DC1";                //设备控制1
            case "DC2":
                return "DC2";                //设备控制2
            case "DC3":
                return "DC3";                //设备控制3
            case "DC4":
                return "DC4";                //设备控制4
            case "NAK":
                return "NAK";                //拒绝接收
            case "SYN":
                return "SYN";                //同步空闲
            case "ETB":
                return "ETB";                //结束传输块
            case "CAN":
                return "CAM";                //取消
            case "EM":
                return "EM";                  //媒介结束
            case "SUB":
                return "SUB";                //代替
            case "ESC":
                return "ESU";                //换码(溢出)
            case "FS":
                return "FS";                  //文件分隔符
            case "GS":
                return "GS";                  //分组符
            case "RS":
                return "RS";                  //记录分隔符
            case "US":
                return "US";                  //单元分隔符
            case "DEL":
                return "DEL";                //删除
            case " ":
                return " ";                    //空格
            case "!=":
                return "NOTEQUAL";
            case "<>":
                return "NOTEQUAL2";
            case ">=":
                return "GREATER_THAN_OR_EQUAL_TO";
            case "<=":
                return "GREATER_OR_EQUAL_TO";
            case "!<":
                return "NOT_LESS_THAN";
            case "!>":
                return "NO_GREATER_THAN";
            default:
                return isNull?null:str;
        }
    }
}