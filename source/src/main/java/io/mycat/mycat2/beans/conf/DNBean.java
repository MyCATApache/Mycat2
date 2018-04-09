package io.mycat.mycat2.beans.conf;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class DNBean {
    private String name;
    private String database;
    private String replica;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getReplica() {
        return replica;
    }

    public void setReplica(String replica) {
        this.replica = replica;
    }



    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((replica == null) ? 0 : replica.hashCode());
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
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (replica == null) {
            if (other.replica != null)
                return false;
        } else if (!replica.equals(other.replica))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DNBean [name=" + name + ", database=" + database + ", replica=" + replica + "]";
    }

}
