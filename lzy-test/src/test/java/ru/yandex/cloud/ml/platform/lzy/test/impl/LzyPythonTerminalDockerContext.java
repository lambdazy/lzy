package ru.yandex.cloud.ml.platform.lzy.test.impl;

import org.testcontainers.containers.GenericContainer;

public class LzyPythonTerminalDockerContext extends LzyTerminalDockerContext {
    public static final String condaPrefix = "eval \"$(conda shell.bash hook)\" && " +
            "conda activate default && ";

    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, String serverAddress, int debugPort, String user, String private_key_path) {
        String terminalCommand = "lzy-terminal ";
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
        final String command = condaPrefix + terminalCommand;
        System.out.println("running command " + command);
        GenericContainer<?> servantContainer = createDockerWithCommandAndModifier(
                user, debugPort, private_key_path, port,
                () -> command,
                (cmd) -> cmd.withEntrypoint("/bin/bash -c")
        );
        return createTerminal(mount, serverAddress, port, servantContainer);
    }
}
