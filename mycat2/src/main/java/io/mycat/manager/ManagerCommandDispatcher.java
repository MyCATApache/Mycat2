package io.mycat.manager;

import com.google.common.collect.ImmutableList;
import io.mycat.DefaultCommandHandler;
import io.mycat.ReceiverImpl;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.manager.commands.*;
import io.mycat.proxy.session.MycatSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ManagerCommandDispatcher extends DefaultCommandHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerCommandDispatcher.class);
    public static final ImmutableList<ManageCommand> COMMANDS = ImmutableList.of(
//            new ShowInstanceCommand(),
//            new ShowReplicaCommand(),
//            new ShowDatasourceCommand(),
//            new ShowConnectionCommand(),
//            new ShowHeartbeatCommand(),
//            new ShowBackendNativeCommand(),
//            new ShowReactorCommand(),
//            new ShowThreadPoolCommand(),
//            new ShowScheduleCommand(),
//            new ShowHelpCommand(),
//            new ShowMetaDataSchemaCommand(),
//            new ShowMetaDataTableCommand(),
//            new KillConnectionCommand(),
//            new SwitchInstanceCommand(),
//            new SwitchReplicaCommand(),
//            new ShowStatCommand(),
//            new ReloadConfigCommand(),
//            new SwitchHeatbeatCommand(),
//            new ShowHeatbeatRunningCommand(),
//            new ResetStatCommand(),
//            new ShowServerCommand()
    );

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        try {
            ////////////////////////////////////////////////////////////////////////////////
            String original = new String(bytes);
            original = original.trim();
            original = original.endsWith(";") ? original.substring(0, original.length() - 1) : original;

            /////////////////////////////////////////////////////////////////////////////////
            MycatRequest mycatRequest = new MycatRequest(session.sessionId(), original, new HashMap<>());

            ReceiverImpl receiver = new ReceiverImpl(session, 1, false, false);

            for (MycatCommand command : COMMANDS) {
                if (command.run(mycatRequest, session.getDataContext(), receiver)) {
                    return;
                }
            }
            LOGGER.info("No matching manager commands:{}", original);
            LOGGER.info("The available management commands are as follows");
            LOGGER.info("statement\tdescription\tclazzName");
            for (ManageCommand command : COMMANDS) {
                String statement = command.statement();
                String description = command.description();
                String clazzName = command.getName();
                LOGGER.info("{}\t{}\t{}", statement, description, clazzName);
            }
            super.handleQuery(bytes, session);
        }catch (Throwable e){
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        }
    }
}