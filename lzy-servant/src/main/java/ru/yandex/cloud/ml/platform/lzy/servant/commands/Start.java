package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantCommand;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;

import java.net.URI;
import java.nio.file.Path;

public class Start implements ServantCommand {
    @Override
    public int execute(CommandLine parse) throws Exception {
        if (!parse.hasOption('z')) {
            throw new RuntimeException("Provide lzy server address with -a option to start a task.");
        }
        String serverAddress = parse.getOptionValue('z');
        if (!serverAddress.contains("//")) {
            serverAddress = "http://" + serverAddress;
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final LzyServant servant = LzyServant.Builder.forLzyServer(URI.create(serverAddress))
            .task(System.getenv("LZYTASK"))
            .token(System.getenv("LZYTOKEN"))
            .servantName(parse.getOptionValue('a', LzyFS.lineCmd("hostname")))
            .servantPort(port)
            .root(Path.of(parse.getOptionValue('m', System.getenv("HOME") + "/.lzy")))
            .build();

        servant.start();
        servant.awaitTermination();
        return 0;
    }
}
