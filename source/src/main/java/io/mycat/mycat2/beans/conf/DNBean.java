package io.mycat.mycat2.beans.conf;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class DNBean {
    private String database;
    private String mysqlReplica;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getMysqlReplica() {
        return mysqlReplica;
    }

    public void setMysqlReplica(String mysqlReplica) {
        this.mysqlReplica = mysqlReplica;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((mysqlReplica == null) ? 0 : mysqlReplica.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DNBean other = (DNBean) obj;
        if (database == null) {
            if (other.database != null)
                return false;
        } else if (!database.equals(other.database))
            return false;
        if (mysqlReplica == null) {
            if (other.mysqlReplica != null)
                return false;
        } else if (!mysqlReplica.equals(other.mysqlReplica))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DNBean [database=" + database + ", mysqlReplica=" + mysqlReplica + "]";
    }
}
