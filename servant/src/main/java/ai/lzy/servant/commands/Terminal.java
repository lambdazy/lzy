package ai.lzy.servant.commands;

import ai.lzy.servant.agents.LzyAgent;
import ai.lzy.servant.agents.LzyAgentConfig;
import ai.lzy.servant.agents.LzyInternalTerminal;
import ai.lzy.servant.agents.LzyTerminal;
import java.io.FileReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.utils.JwtCredentials;
import ai.lzy.fs.fs.LzyFS;

public class Terminal implements LzyCommand {
    private static final Options options = new Options();

    static {
        options.addOption("d", "direct", false, "Use direct server connection instead of Kharon");
        options.addOption("u", "user", true, "User for direct connection");
        options.addOption("t", "user-token", true, "User token for direct connection");
    }

    @Override
    public int execute(CommandLine parse) throws Exception {
        if (!parse.hasOption('z')) {
            throw new IllegalArgumentException(
                "Provide lzy server address with -z option to start a task.");
        }

        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, parse.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("terminal", options);
            return -1;
        }

        String serverAddress = parse.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final int fsPort = Integer.parseInt(parse.getOptionValue('q', "9998"));

        final Path lzyRoot = Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy"));
        Runtime.getRuntime().exec("umount " + lzyRoot);
        final String host = parse.getOptionValue('h', LzyFS.lineCmd("hostname"));
        final LzyAgentConfig.LzyAgentConfigBuilder builder = LzyAgentConfig.builder()
            .serverAddress(URI.create(serverAddress))
            .whiteboardAddress(URI.create(parse.getOptionValue("w")))
            .user(System.getenv("USER"))
            .agentHost(host)
            .agentPort(port)
            .fsPort(fsPort)
            .root(lzyRoot);

        final boolean direct = localCmd.hasOption('d');

        if (direct) {
            builder.user(localCmd.getOptionValue("u", System.getenv("USER")));
            builder.token(localCmd.getOptionValue("t", ""));
        } else {
            final Path privateKeyPath = Paths.get(parse.getOptionValue('k', System.getenv("HOME") + "/.ssh/id_rsa"));
            if (Files.exists(privateKeyPath)) {
                try (FileReader keyReader = new FileReader(String.valueOf(privateKeyPath))) {
                    String token = JwtCredentials.buildJWT(System.getenv("USER"), keyReader);
                    builder.token(token);
                }
            } else {
                builder.token("");
            }
        }

        final LzyAgentConfig agentConfig = builder.build();
        final LzyAgent terminal = direct ? new LzyInternalTerminal(agentConfig) : new LzyTerminal(agentConfig);

        terminal.start();
        terminal.awaitTermination();
        return 0;
    }
}
