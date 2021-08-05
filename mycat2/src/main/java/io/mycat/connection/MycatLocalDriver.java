package io.mycat.connection;

import io.mycat.MycatDataContext;
import io.mycat.commands.MycatdbCommand;
import io.mycat.commands.ReceiverImpl;
import io.mycat.runtime.MycatDataContextImpl;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

public class MycatLocalDriver extends AbstractDriver {
    static {
        try {
            DriverManager.registerDriver(new MycatLocalDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion("Mycat JDBC driver", "0.1", "Mycat", "0.1", true, 0, 1, 0, 1);
    }

    @Override
    public Iterable<? extends ConnectionProperty> getConnectionProperties() {
        return Collections.emptyList();
    }

    @Override
    public String getConnectStringPrefix() {
        return  "jdbc:mycat:";
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        MycatDataContext dataContext = new MycatDataContextImpl();
        return new MycatConnection(dataContext);
    }
}
