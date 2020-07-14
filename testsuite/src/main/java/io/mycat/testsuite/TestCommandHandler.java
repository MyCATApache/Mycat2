package io.mycat.testsuite;

import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;

import java.util.List;

public class TestCommandHandler implements CommandHandler {
    @Override
    public Command parseCommand(List<String> lines, List<String> content, String line) {
        return null;
    }
}