package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
import io.mycat.util.SQLHanlder;

public enum MycatdbCommand implements MycatCommand {
    INSTANCE;

    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        MycatDBClientMediator client = MycatDBs.createClient(context);
        SQLHanlder sqlHanlder = new SQLHanlder(client.sqlContext());
        sqlHanlder.parse(request.getText(), response);
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        MycatDBClientMediator client = MycatDBs.createClient(context);
        SQLHanlder sqlHanlder = new SQLHanlder(client.sqlContext());
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return "mycatdb";
    }
}