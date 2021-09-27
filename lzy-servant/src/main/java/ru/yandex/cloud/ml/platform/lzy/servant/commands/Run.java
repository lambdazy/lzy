package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

public class Run implements ServantCommand {
    private static final Logger LOG = LogManager.getLogger(Run.class);
    private static final int BUFFER_SIZE = 4096;
    private static final Options options = new Options();

    static {
        options.addOption(new Option("m", "mapping", true, "Slot-channel mapping"));
    }

    private final Set<String> tempChannels = new HashSet<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private LzyServerGrpc.LzyServerBlockingStub server;
    private IAM.Auth auth;
    private Map<String, Map<String, String>> pipesConfig;
    private LzyServantGrpc.LzyServantBlockingStub servant;
    private long pid;
    private String lzyRoot;

    private final CountDownLatch communicationLatch = new CountDownLatch(3);

    @Override
    public int execute(CommandLine command) throws Exception {
        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("channel", options);
            return -1;
        }
        final Map<String, String> bindings = new HashMap<>();
        if (localCmd.hasOption('m')) {
            //Slot name -> Channel ID
            //noinspection unchecked
            bindings.putAll(objectMapper.readValue(new File(localCmd.getOptionValue('m')), Map.class));
        }

        lzyRoot = command.getOptionValue('m');
        pid = ProcessHandle.current().pid();
        pipesConfig = pipesConfig();

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        {
            final ManagedChannel serverCh = ManagedChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .build();
            server = LzyServerGrpc.newBlockingStub(serverCh);
        }
        {
            final ManagedChannel servantCh = ManagedChannelBuilder
                .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
                .usePlaintext()
                .build();
            servant = LzyServantGrpc.newBlockingStub(servantCh);
        }

        final Operations.Zygote.Builder builder = Operations.Zygote.newBuilder();
        JsonFormat.parser().merge(System.getenv("ZYGOTE"), builder);
        final Operations.Zygote grpcZygote = builder.build();
        final Zygote zygote = gRPCConverter.from(grpcZygote);
        final Tasks.TaskSpec.Builder taskSpec = Tasks.TaskSpec.newBuilder();
        taskSpec.setAuth(auth);
        taskSpec.setZygote(grpcZygote);
        zygote.slots().forEach(slot -> {
            final String binding;
            if (slot.media() == Slot.Media.ARG) {
                binding = String.join(" ", command.getArgList().subList(1, command.getArgList().size()));
            } else if (bindings.containsKey(slot.name())) {
                binding = "channel:" + bindings.get(slot.name());
            } else {
                binding = "channel:" + resolveChannel(slot);
            }
            taskSpec.addAssignmentsBuilder()
                .setSlot(gRPCConverter.to(slot))
                .setBinding(binding)
                .build();
        });

        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
        final Iterator<Servant.ExecutionProgress> executionProgress = server.start(taskSpec.build());
        executionProgress.forEachRemaining(progress -> {
            try {
                LOG.info(JsonFormat.printer().print(progress));
                if (progress.hasDetach() && "/dev/stdin".equals(progress.getDetach().getSlot().getName())) {
                    System.in.close();
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.warn("Unable to parse execution progress", e);
            } catch (IOException e) {
                LOG.error("Failed to close stdin", e);
            }
        });
        communicationLatch.await(); // waiting for slots to finish communication
        return 0;
    }

    private Map<String, Map<String, String>> pipesConfig() throws IOException {
        final Process p = Runtime.getRuntime().exec("lsof -p " + pid + " -a -d0,1,2 -F ftidn");
        final InputStreamReader inputStreamReader = new InputStreamReader(
            p.getInputStream(),
            StandardCharsets.UTF_8
        );
        String[] fdNames = new String[]{"stdin", "stdout", "stderr"};
        final Map<String, Map<String, String>> pipeMappings = new HashMap<>();
        try (final LineNumberReader lineNumberReader = new LineNumberReader(inputStreamReader)) {
            String line;
            String name = null;
            final Map<Character, String> namesMappings = Map.of(
                't', "type",
                'd', "device",
                'i', "node",
                'n', "name"
            );
            while ((line = lineNumberReader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                switch (line.charAt(0)) {
                    case 'p':
                        break;
                    case 'f':
                        if (!Character.isDigit(line.charAt(line.length() - 1))) {
                            line = line.substring(0, line.length() - 1);
                        }
                        name = fdNames[Integer.parseInt(line.substring(1))];
                        break;
                    case 'n':
                        if (line.substring(1).startsWith("->")) {
                            line = 'n' + line.substring(3);
                        }
                    default:
                        pipeMappings.computeIfAbsent(name, n -> new HashMap<>())
                            .put(namesMappings.getOrDefault(line.charAt(0), "unknown"), line.substring(1));
                        break;
                }
            }
        }
        return pipeMappings;
    }

    private String resolveChannel(Slot slot) {
        LOG.info("Creating custom slot " + slot);
        final String prefix = (auth.hasTask() ? auth.getTask().getTaskId() : auth.getUser().getUserId()) + ":" + pid;
        if (slot.name().startsWith("/dev/")) {
            final String devSlot = slot.name().substring("/dev/".length());
            final Map<String, String> pipeConfig = pipesConfig.get(devSlot);
            switch (devSlot) {
                case "stdin": {
                    final String channelName;
                    boolean pipe = false;
                    if (!pipeConfig.getOrDefault("node", "").isEmpty()) { // linux
                        channelName = prefix + ":" + devSlot + ":" + pipeConfig.get("node");
                        pipe = true;
                    } else if (!pipeConfig.getOrDefault("name", "").isEmpty()) { // macos
                        channelName = prefix + ":" + devSlot + ":" + pipeConfig.get("name");
                        pipe = true;
                    } else {
                        channelName = UUID.randomUUID().toString();
                    }

                    final String slotName = String.join("/", "/tasks", prefix, devSlot);
                    createChannel(slot, channelName);

                    createSlotByProto(prefix + ":" + devSlot, pipe, "channel:" + channelName, slotName, Slot.STDOUT);
                    final Path inputSlotFile = Path.of(lzyRoot, slotName);
                    ForkJoinPool.commonPool().execute(() -> {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (OutputStream is = Files.newOutputStream(inputSlotFile, StandardOpenOption.WRITE)) {
                            int read;
                            while ((read = System.in.read(buffer)) >= 0) {
                                is.write(buffer, 0, read);
                            }
                        } catch (IOException e) {
                            LOG.warn("Unable to read from input stream", e);
                        }
                        communicationLatch.countDown();
                    });
                    return channelName;
                }
                case "stdout":
                case "stderr": {
                    final String channelName;
                    boolean pipe = false;
                    if (!pipeConfig.getOrDefault("node", "").isEmpty()) { // linux
                        channelName = prefix + ":" + devSlot + ":" + pipeConfig.get("node");
                        pipe = true;
                    } else if (pipeConfig.getOrDefault("device", "").startsWith("0x")) { // macos
                        channelName = prefix + ":" + devSlot + ":" + pipeConfig.get("name");
                        pipe = true;
                    } else {
                        channelName = UUID.randomUUID().toString();
                    }

                    final String slotName = String.join("/", "/tasks", prefix, devSlot);
                    final String channelId = createChannel(slot, channelName);

                    createSlotByProto(prefix + ":" + devSlot, pipe, channelId, slotName, Slot.STDIN);

                    final Path outputSlotFile = Path.of(lzyRoot, slotName);
                    ForkJoinPool.commonPool().execute(() -> {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (InputStream is = Files.newInputStream(outputSlotFile, StandardOpenOption.READ)) {
                            int read;
                            while ((read = is.read(buffer)) >= 0) {
                                ("stderr".equals(devSlot) ? System.err : System.out).write(buffer, 0, read);
                            }
                        } catch (IOException e) {
                            LOG.warn("Unable to read from " + devSlot, e);
                        }
                        communicationLatch.countDown();
                    });
                    return channelId;
                }
                default:
                    throw new IllegalArgumentException(
                        MessageFormat.format("Illegal slot found: {0}", slot.name())
                    );
            }
        } else {
            throw new IllegalArgumentException(
                MessageFormat.format("Slot {0} assignment is not specified", slot.name())
            );
        }
    }

    private void createSlotByProto(
        String name,
        boolean pipe,
        String channelId,
        String slotName,
        Slot slotProto
    ) {
        try {
            final Operations.Slot slotDeclaration = Operations.Slot.newBuilder(gRPCConverter.to(slotProto))
                .setName(slotName)
                .build();
            //noinspection ResultOfMethodCallIgnored
            servant.configureSlot(Servant.SlotCommand.newBuilder()
                .setSlot(name)
                .setCreate(Servant.CreateSlotCommand.newBuilder()
                    .setSlot(slotDeclaration)
                    .setIsPipe(pipe)
                    .setChannelId(channelId)
                    .build()
                )
                .build()
            );
        } catch (Exception e) {
            LOG.warn("Unable to create slot: " + slotName, e);
        }
    }

    private String createChannel(Slot slot, String channelName) {
        tempChannels.add(channelName);
        final Channels.ChannelStatus channel = server.channel(Channels.ChannelCommand.newBuilder()
            .setAuth(auth)
            .setChannelName(channelName)
            .setCreate(Channels.ChannelCreate.newBuilder()
                .setContentType(gRPCConverter.to(slot.contentType()))
                .build())
            .build()
        );
        return channel.getChannel().getChannelId();
    }

    private void onShutdown() {
        tempChannels.forEach(channel -> {
            LOG.info("Run::server.channel destroy");
            //noinspection ResultOfMethodCallIgnored
            server.channel(Channels.ChannelCommand.newBuilder()
                .setAuth(auth)
                .setChannelName(channel)
                .setDestroy(Channels.ChannelDestroy.newBuilder().build())
                .build()
            );
        });
    }
}
