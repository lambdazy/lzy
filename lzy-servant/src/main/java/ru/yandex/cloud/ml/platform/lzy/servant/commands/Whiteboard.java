package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.*;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

import java.net.URI;
import java.util.*;
import java.util.List;

public class Whiteboard implements LzyCommand {
    private static final Options options = new Options();

    static {
        options.addOption(new Option("e", "entry", true, "Entry ID for mapping"));
        options.addOption(new Option("f", "field", true, "Whiteboard field for mapping"));
        options.addOption(new Option("l", "fields list", true, "Whiteboard fields comma-separated list"));
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
        if (command.getArgs().length < 2)
            throw new IllegalArgumentException("Please specify whiteboard command");
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ManagedChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .build();
        final LzyKharonGrpc.LzyKharonBlockingStub server = LzyKharonGrpc.newBlockingStub(serverCh);
        switch (command.getArgs()[1]) {
            case "create": {
                if (!localCmd.hasOption('l')) {
                    throw new IllegalArgumentException("Whiteboard fields list must be specified");
                }
                final List<String> fields = List.of(localCmd.getOptionValue('l').split(","));
                final LzyWhiteboard.Whiteboard whiteboardId = server.createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand
                        .newBuilder()
                        .setSnapshotId(command.getArgs()[2])
                        .addAllFieldNames(fields)
                        .setAuth(auth)
                        .build()
                );
                System.out.println(JsonFormat.printer().print(whiteboardId));
                break;
            }
            case "link": {
                if (!localCmd.hasOption('e') || !localCmd.hasOption('f')) {
                    throw new IllegalArgumentException("Add link command requires entry ID and whiteboard field");
                }
                final String entryId = localCmd.getOptionValue('e');
                final String wbField = localCmd.getOptionValue('f');
                final LzyWhiteboard.OperationStatus operationStatus = server.addLink(LzyWhiteboard.LinkCommand
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
                final LzyWhiteboard.Whiteboard whiteboard = server.getWhiteboard(LzyWhiteboard.GetWhiteboardCommand
                        .newBuilder()
                        .setWhiteboardId(command.getArgs()[2])
                        .setAuth(auth)
                        .build()
                );
                System.out.println(JsonFormat.printer().print(whiteboard));
                break;
            }
        }
        return 0;
    }
}
