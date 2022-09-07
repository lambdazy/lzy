package ai.lzy.servant.commands;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.fs.fs.LzyFS;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyInternalTerminal;
import ai.lzy.servant.agents.LzyTerminal;
import ai.lzy.util.auth.credentials.JwtUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.FileReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Terminal implements LzyCommand {
    private static final Options options = new Options();

    static {
        options.addOption("d", "direct", false, "Use direct server connection instead of Kharon");
        options.addOption("u", "user", true, "User for direct connection");
        options.addOption("t", "user-token", true, "User token for direct connection");
    }

    @Override
    public int execute(CommandLine commandLine) throws Exception {
        if (!commandLine.hasOption('z')) {
            throw new IllegalArgumentException(
                "Provide lzy server address with -z option to start a task.");
        }

        final CommandLine localCmd = parse(commandLine, options);
        if (localCmd == null) {
            return -1;
        }

        String serverAddress = commandLine.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }
        final int port = Integer.parseInt(commandLine.getOptionValue('p', "9999"));
        final int fsPort = Integer.parseInt(commandLine.getOptionValue('q', "9998"));

        final Path lzyRoot = Path.of(commandLine.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        Runtime.getRuntime().exec("umount " + lzyRoot);
        final String host = commandLine.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final LzyAgentConfig.LzyAgentConfigBuilder builder = LzyAgentConfig.builder()
            .serverAddress(URI.create(serverAddress))
            .whiteboardAddress(URI.create(commandLine.getOptionValue("w")))
            .user(System.getenv("USER"))
            .scheme("terminal")
            .agentHost(host)
            .agentPort(port)
            .fsPort(fsPort)
            .root(lzyRoot);

        final boolean direct = localCmd.hasOption('d');

        if (direct) {
            builder.user(localCmd.getOptionValue("u", System.getenv("USER")));
            builder.token(localCmd.getOptionValue("t", ""));
        } else {
            final Path privateKeyPath = Paths.get(
                commandLine.getOptionValue('k', System.getenv("HOME") + "/.ssh/id_rsa"));
            if (Files.exists(privateKeyPath)) {
                try (FileReader keyReader = new FileReader(String.valueOf(privateKeyPath))) {
                    String token = JwtUtils.legacyBuildJWT(System.getenv("USER"), keyReader);
                    builder.token(token);
                }
            } else {
                builder.token("");
            }
        }

        final LzyAgentConfig agentConfig = builder.build();
        if (direct) {
            final LzyInternalTerminal terminal = new LzyInternalTerminal(agentConfig);
            terminal.awaitTermination();
        } else {
            final LzyTerminal terminal = new LzyTerminal(agentConfig);
            terminal.awaitTermination();
        }
        return 0;
    }
}
