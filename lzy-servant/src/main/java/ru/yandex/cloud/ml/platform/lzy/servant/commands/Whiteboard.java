package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

public class Whiteboard implements LzyCommand {

    private static final Options options = new Options();

    static {
        options.addOption(new Option("e", "entry", true, "Entry ID for mapping"));
        options.addOption(new Option("f", "field", true, "Whiteboard field for mapping"));
        options.addOption(
            new Option("l", "fields list", true, "Whiteboard fields comma-separated list"));
        options.addOption(
                new Option("t", "tags list", true, "Whiteboard tags comma-separated list"));
        options.addOption(new Option("n", "namespace", true, "Whiteboard namespace"));
    }

    @Override
    public int execute(CommandLine command) throws Exception {
        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("whiteboard", options);
            return -1;
        }
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify whiteboard command");
        }
        final IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyKharonGrpc.LzyKharonBlockingStub server = LzyKharonGrpc.newBlockingStub(serverCh);
        switch (command.getArgs()[1]) {
            case "create": {
                if (!localCmd.hasOption('l')) {
                    throw new IllegalArgumentException("Whiteboard fields list must be specified");
                }
                if (!localCmd.hasOption('n')) {
                    throw new IllegalArgumentException("Whiteboard namespace must be specified");
                }
                List<String> tags = new ArrayList<>();
                if (localCmd.hasOption('t')) {
                    tags = List.of(localCmd.getOptionValue('t').split(","));
                }
                final List<String> fields = List.of(localCmd.getOptionValue('l').split(","));
                final String namespace = localCmd.getOptionValue('n');
                final LzyWhiteboard.Whiteboard whiteboardId = server
                    .createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand
                        .newBuilder()
                        .setSnapshotId(command.getArgs()[2])
                        .addAllFieldNames(fields)
                        .addAllTags(tags)
                        .setNamespace(namespace)
                        .setAuth(auth)
                        .build()
                    );
                System.out.println(JsonFormat.printer().print(whiteboardId));
                break;
            }
            case "link": {
                if (!localCmd.hasOption('e') || !localCmd.hasOption('f')) {
                    throw new IllegalArgumentException(
                        "Add link command requires entry ID and whiteboard field");
                }
                final String entryId = localCmd.getOptionValue('e');
                final String wbField = localCmd.getOptionValue('f');
                final LzyWhiteboard.OperationStatus operationStatus = server
                    .addLink(LzyWhiteboard.LinkCommand
                        .newBuilder()
                        .setWhiteboardId(command.getArgs()[2])
                        .setAuth(auth)
                        .setEntryId(entryId)
                        .setFieldName(wbField)
                        .build()
                    );
                System.out.println(JsonFormat.printer().print(operationStatus));
                break;
            }
            case "get": {
                final LzyWhiteboard.Whiteboard whiteboard = server
                    .getWhiteboard(LzyWhiteboard.GetWhiteboardCommand
                        .newBuilder()
                        .setWhiteboardId(command.getArgs()[2])
                        .setAuth(auth)
                        .build()
                    );
                System.out.println(JsonFormat.printer().print(whiteboard));
                break;
            }
            case "list": {
                if (!localCmd.hasOption('n')) {
                    throw new IllegalArgumentException("Whiteboard namespace must be specified");
                }
                final String namespace = localCmd.getOptionValue('n');
                List<String> tags = new ArrayList<>();
                if (localCmd.hasOption('t')) {
                    tags = List.of(localCmd.getOptionValue('t').split(","));
                }
                final LzyWhiteboard.WhiteboardsResponse whiteboards =
                    server.whiteboardByNamespaceAndTags(LzyWhiteboard.WhiteboardByNamespaceAndTagsCommand
                        .newBuilder()
                        .setAuth(auth)
                        .addAllTags(tags)
                        .setNamespace(namespace)
                        .build()
                    );
                System.out.println(JsonFormat.printer().print(whiteboards));
            }
        }
        return 0;
    }
}
