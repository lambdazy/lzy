package ru.yandex.cloud.ml.platform.lzy.test.impl;

import static org.testcontainers.shaded.org.apache.commons.lang.SystemUtils.IS_OS_LINUX;

import org.testcontainers.containers.GenericContainer;

public class LzyPythonTerminalDockerContext extends LzyTerminalDockerContext {
    public static final String condaPrefix = "eval \"$(conda shell.bash hook)\" && " +
            "conda activate default && ";

    @Override
    public Terminal startTerminalAtPathAndPort(String mount, int port, String serverAddress, int debugPort, String user, String private_key_path) {
        final String internalHost = IS_OS_LINUX ? "localhost" : "host.docker.internal";
        String terminalCommand = "";
        if (serverAddress != null) {
            terminalCommand += "-s " + serverAddress + " ";
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
        terminalCommand += "-p " + port + " ";
        terminalCommand += "-i " + internalHost + " ";
        terminalCommand += "-d " + debugPort + " ";
        final String command = terminalCommand;
        System.out.println("running command " + command);
        GenericContainer<?> servantContainer = createDockerWithCommandAndModifier(
                user, debugPort, private_key_path, port,
                () -> command,
                (cmd) -> cmd.withEntrypoint("/test_entrypoint.sh")
        );
        return createTerminal(mount, serverAddress, port, servantContainer);
    }
}
