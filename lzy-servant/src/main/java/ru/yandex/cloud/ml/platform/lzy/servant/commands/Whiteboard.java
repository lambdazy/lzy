package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.*;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

import java.io.File;
import java.net.URI;
import java.util.*;

public class Whiteboard implements LzyCommand {
    private static final Options options = new Options();

    static {
        options.addOption(new Option("m", "mapping", true, "Field name <-> entryId mapping"));
    }

    private final ObjectMapper objectMapper = new ObjectMapper();

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
                final LzyWhiteboard.WhiteboardId whiteboardId = server.createWhiteboard(LzyWhiteboard.CreateWhiteboardCommand
                        .newBuilder()
                        .setSnapshotId(command.getArgs()[2])
                        .setUserCredentials(auth.getUser())
                        .build()
                );
                System.out.println(JsonFormat.printer().print(whiteboardId));
                break;
            }
            case "link": {
                final Map<String, String> mapping = new HashMap<>();
                if (localCmd.hasOption('m')) {
                    final String mappingFile = localCmd.getOptionValue('m');
                    //noinspection unchecked
                    mapping.putAll(objectMapper.readValue(new File(mappingFile), Map.class));
                }
                if (!localCmd.hasOption('m') || mapping.isEmpty()) {
                    throw new IllegalArgumentException("Add link command requires -m argument that points to non-empty mappings");
                }
                final List<LzyWhiteboard.FieldMapping> fmList = new ArrayList<>();
                for (var entry : mapping.entrySet()) {
                    fmList.add(LzyWhiteboard.FieldMapping
                            .newBuilder()
                            .setFieldName(entry.getKey())
                            .setEntryId(entry.getValue())
                            .build()
                    );
                }
                final LzyWhiteboard.OperationStatus operationStatus = server.addLink(LzyWhiteboard.AddLinkCommand
                        .newBuilder()
                        .setWbId(command.getArgs()[2])
                        .setAuth(auth.getUser())
                        .addAllMappings(fmList)
                        .build()
                );
                System.out.println(JsonFormat.printer().print(operationStatus));
                break;
            }
            case "get": {
                final LzyWhiteboard.Whiteboard whiteboard = server.getWhiteboard(LzyWhiteboard.GetWhiteboardCommand
                        .newBuilder()
                        .setWbId(command.getArgs()[2])
                        .setAuth(auth.getUser())
                        .build()
                );
                System.out.println(JsonFormat.printer().print(whiteboard));
                break;
            }
        }
        return 0;
    }
}
