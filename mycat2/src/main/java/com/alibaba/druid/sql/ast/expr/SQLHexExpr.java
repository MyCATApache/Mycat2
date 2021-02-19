//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.druid.sql.ast.expr;

import com.alibaba.druid.FastsqlException;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.druid.util.HexBin;
import java.io.IOException;

public class SQLHexExpr extends SQLExprImpl implements SQLLiteralExpr, SQLValuableExpr {
    private final String hex;

    public SQLHexExpr(String hex) {
        this.hex = hex;
    }

    public String getHex() {
        return this.hex;
    }

    public void output(Appendable buf) {
        try {
            buf.append("x'");
            buf.append(this.hex);
            buf.append("'");
            String charset = (String)this.getAttribute("USING");
            if (charset != null) {
                buf.append(" USING ");
                buf.append(charset);
            }

        } catch (IOException var3) {
            throw new FastsqlException("output error", var3);
        }
    }

    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public int hashCode() {
        int prime = 1;
        int result = 1;
         result = 31 * result + (this.hex == null ? 0 : this.hex.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            SQLHexExpr other = (SQLHexExpr)obj;
            if (this.hex == null) {
                if (other.hex != null) {
                    return false;
                }
            } else if (!this.hex.equals(other.hex)) {
                return false;
            }

            return true;
        }
    }

    public byte[] toBytes() {
        return HexBin.decode(this.hex);
    }

    public SQLHexExpr clone() {
        return new SQLHexExpr(this.hex);
    }

    public byte[] getValue() {
        return this.toBytes();
    }

    public SQLCharExpr toCharExpr() {
        byte[] bytes = this.toBytes();
        if (bytes == null) {
            return null;
        } else {
            String str = new String(bytes, SQLUtils.UTF8);
            return new SQLCharExpr(str);
        }
    }
}
