package io.mycat.connection;

import org.apache.calcite.avatica.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public  abstract class AbstractDriver implements Driver {

    final DriverVersion version = this.createDriverVersion();

    protected abstract DriverVersion createDriverVersion();
    public abstract Iterable<? extends ConnectionProperty> getConnectionProperties();
    public   abstract String getConnectStringPrefix() ;

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(getConnectStringPrefix());
    }


    /**
     * org.apache.calcite.avatica.UnregisteredDriver#getPropertyInfo(java.lang.String, java.util.Properties)
     *
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();

        // First, add the contents of info
        for (Map.Entry<Object, Object> entry : info.entrySet()) {
            list.add(
                    new DriverPropertyInfo(
                            (String) entry.getKey(),
                            (String) entry.getValue()));
        }
        // Next, add property definitions not mentioned in info
        for (ConnectionProperty p : getConnectionProperties()) {
            if (info.containsKey(p.name())) {
                continue;
            }
            list.add(new DriverPropertyInfo(p.name(), null));
        }
        return list.toArray(new DriverPropertyInfo[list.size()]);
    }



    @Override
    public int getMajorVersion() {
        return version.majorVersion;
    }

    @Override
    public int getMinorVersion() {
        return version.minorVersion;
    }

    @Override
    public boolean jdbcCompliant() {
        return version.jdbcCompliant;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return  Logger.getLogger("");
    }
    protected void register() {
        try {
            DriverManager.registerDriver(this);
        } catch (SQLException e) {
            System.out.println(
                    "Error occurred while registering JDBC driver "
                            + this + ": " + e.toString());
        }
    }
}
