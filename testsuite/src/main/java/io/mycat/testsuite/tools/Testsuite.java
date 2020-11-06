package io.mycat.testsuite.tools;

import net.hydromatic.quidem.Quidem;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Testsuite {
    public static void main(String[] args) throws Throwable {
        String testoutput = null;
        if (args.length == 0) {
            String connectionFactoryName = TestConnectionFactory.class.getCanonicalName();
            String commandHandlerName = TestCommandHandler.class.getCanonicalName();
            String input = Paths.get(Testsuite.class.getResource("/example.iq").toURI()).toAbsolutePath().toString();
            testoutput = Files.createTempFile("testoutput", ".txt").toAbsolutePath().toString();
            System.out.println(testoutput);
            args = new String[]{"--factory", connectionFactoryName,
                    "--command-handler", commandHandlerName, input, testoutput};
        }
        Quidem.main(args);
    }
}