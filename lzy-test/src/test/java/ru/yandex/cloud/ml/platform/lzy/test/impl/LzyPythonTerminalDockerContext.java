package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.testcontainers.containers.GenericContainer;

public class LzyPythonTerminalDockerContext extends LzyTerminalDockerContext {
    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, String serverAddress, int debugPort, String user, String private_key_path) {
        String terminalCommand = "terminal ";
        if (serverAddress != null) {
            terminalCommand += "--url " + serverAddress + " ";
        }
        if (private_key_path != null) {
            terminalCommand += "-k " + private_key_path + " ";
        }
        if (mount != null)  {
            terminalCommand += "-m "  + mount + " ";
        }
        if (user != null) {
            terminalCommand += "-u " + user + " ";
        }
        terminalCommand += ";";
        final String command = terminalCommand;

        GenericContainer<?> servantContainer = createDockerWithCommandAndModifier(
                user, debugPort, private_key_path, port,
                () -> command,
                (genericContainer -> genericContainer.withCreateContainerCmdModifier(
                        (cmd) -> cmd.withEntrypoint("/bin/bash -c")
                ))
        );
        return createTerminal(mount, serverAddress, port, servantContainer);
    }
}
