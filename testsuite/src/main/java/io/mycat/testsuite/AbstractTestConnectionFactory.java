package io.mycat.testsuite;

import net.hydromatic.quidem.Quidem;

import java.sql.Connection;
import java.util.function.Supplier;

public class AbstractTestConnectionFactory implements Quidem.ConnectionFactory  {
    Supplier<SimpleConnection> factory;

    public AbstractTestConnectionFactory(Supplier<SimpleConnection> factory) {
        this.factory = factory;
    }

    @Override
    public Connection connect(String name, boolean reference) throws Exception {
        if (!reference){
            SimpleConnection connection = factory.get();
            if (name!=null){
                connection.useSchema(name);
            }
            return new TestConnection(connection);
        }
        return null;
    }
}