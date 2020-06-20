package io.mycat.manager.commands;

import io.mycat.MySQLTaskUtil;
import io.mycat.MycatCore;
import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillConnectionCommand implements ManageCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillConnectionCommand.class);
    @Override
    public String statement() {
        return "kill @@connection id1,id2";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        String text = request.getText();
        for (String s : text.replace("kill @@connection ", "").split(",")) {
            int id = Integer.parseInt(s);
            for (MycatReactorThread mycatReactorThread : MycatCore.INSTANCE.getReactorManager().getList()) {
                SessionManager.FrontSessionManager<MycatSession> frontManager = mycatReactorThread.getFrontManager();
                MycatSession mycatSession = frontManager.getAllSessions().stream().filter(i -> i.sessionId() == id).findFirst().orElse(null);
                if (mycatSession!=null){
                    LOGGER.info("prepare kill "+id);
                    mycatReactorThread.addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            mycatSession.close(false,text);
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {

                        }

                        @Override
                        public String message() {
                            return text;
                        }
                    });
                }
                MySQLClientSession mySQLClientSession = mycatReactorThread.getMySQLSessionManager().getAllSessions().stream().filter(i -> i.sessionId() == id).findFirst().orElse(null);
                if (mySQLClientSession!=null){
                    LOGGER.info("prepare kill "+id);
                    mycatReactorThread.addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            mySQLClientSession.close(false,text);
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {

                        }

                        @Override
                        public String message() {
                            return text;
                        }
                    });
                }
            }

        }
    }

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if (request.getText().toLowerCase().startsWith("kill @@connection ")){
            handle(request, context, response);
        }
        return false;
    }
}