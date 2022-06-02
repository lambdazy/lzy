package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.testcontainers.containers.GenericContainer;

@Deprecated
public class LzyPythonTerminalDockerContext extends TerminalDockerContext {
    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, int fsPort, String serverAddress, int debugPort,
                                               String user, String private_key_path) {
        String terminalCommand = "";
        if (serverAddress != null) {
            terminalCommand += "-s " + serverAddress + " ";
        }
        if (private_key_path != null) {
            terminalCommand += "-k " + private_key_path + " ";
        }
        if (mount != null) {
            terminalCommand += "-m " + mount + " ";
        }
        if (user != null) {
            terminalCommand += "-u " + user + " ";
        }
        terminalCommand += "-p " + port + " ";
        terminalCommand += "-q " + fsPort + " ";
        terminalCommand += "-d " + debugPort + " ";
        final String command = terminalCommand;
        System.out.println("running command " + command);
        GenericContainer<?> servantContainer = createDockerWithCommandAndModifier(
            user, debugPort, private_key_path, port,
            () -> command,
            (cmd) -> cmd.withEntrypoint("/test_entrypoint.sh")
        );
        return createTerminal(mount, serverAddress, port, fsPort, servantContainer);
    }
}
