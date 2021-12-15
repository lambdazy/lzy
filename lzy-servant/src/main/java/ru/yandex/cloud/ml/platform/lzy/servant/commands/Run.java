package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class Run implements LzyCommand {
    private static final Logger LOG = LogManager.getLogger(Run.class);
    private static final int BUFFER_SIZE = 4096;
    private static final Options options = new Options();

    static {
        options.addOption(new Option("m", "mapping", true, "Slot-channel mapping"));
        options.addOption(new Option("s", "slot-mapping", true, "Slot-entryId mapping"));
        options.addOption(new Option("n", "name", true, "Task name"));
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private LzyKharonGrpc.LzyKharonBlockingStub kharon;
    private IAM.Auth auth;
    private Map<String, Map<String, String>> pipesConfig;
    private LzyServantGrpc.LzyServantBlockingStub servant;
    private long pid;
    private String lzyRoot;
    private String stdinChannel;

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
            final String mappingFile = localCmd.getOptionValue('m');
            LOG.info("Read mappings from file " + mappingFile);
            //Slot name -> Channel ID
            //noinspection unchecked
            bindings.putAll(objectMapper.readValue(new File(mappingFile), Map.class));
            LOG.info("Bindings: " +
                bindings.entrySet().stream()
                    .map(e -> e.getKey() + " -> " + e.getValue())
                    .collect(Collectors.joining(";\n"))
            );
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
            kharon = LzyKharonGrpc.newBlockingStub(serverCh);
        }
        {
            final ManagedChannel servant = ManagedChannelBuilder
                .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
                .usePlaintext()
                .build();
            this.servant = LzyServantGrpc.newBlockingStub(servant);
        }

        final Operations.Zygote.Builder builder = Operations.Zygote.newBuilder();
        JsonFormat.parser().merge(System.getenv("ZYGOTE"), builder);
        final Operations.Zygote grpcZygote = builder.build();
        final Zygote zygote = gRPCConverter.from(grpcZygote);
        final Tasks.TaskSpec.Builder taskSpec = Tasks.TaskSpec.newBuilder();
        taskSpec.setAuth(auth);
        taskSpec.setZygote(grpcZygote);
        if (localCmd.hasOption('s')) {
            final String mappingsFile = localCmd.getOptionValue('s');
            //noinspection unchecked
            final Map<String, String> mappings = new HashMap<String, String>(objectMapper.readValue(new File(mappingsFile), Map.class));
            final List<Tasks.SlotMapping> slotMappings = new ArrayList<>();
            for (var entry : mappings.entrySet()) {
                slotMappings.add(Tasks.SlotMapping
                    .newBuilder()
                    .setSlotName(entry.getKey())
                    .setEntryId(entry.getValue())
                    .build());
            }
            taskSpec.setSnapshotMeta(Tasks.SnapshotMeta.newBuilder().addAllMappings(slotMappings).build());
        }
        zygote.slots().forEach(slot -> {
            LOG.info("Resolving slot " + slot.name());
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

        final long startTimeMillis = System.currentTimeMillis();
        final Iterator<Servant.ExecutionProgress> executionProgress = kharon.start(taskSpec.build());
        final Servant.ExecutionConcluded[] exit = new Servant.ExecutionConcluded[1];
        exit[0] = Servant.ExecutionConcluded.newBuilder()
            .setRc(-1)
            .setDescription("Got no exit code from servant")
            .build();
        executionProgress.forEachRemaining(progress -> {
            try {
                LOG.info(JsonFormat.printer().print(progress));
                if (progress.hasDetach() && "/dev/stdin".equals(progress.getDetach().getSlot().getName())) {
                    LOG.info("Closing stdin");
                    System.in.close();
                }
                if (progress.hasExit()) {
                    exit[0] = progress.getExit();
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.warn("Unable to parse execution progress", e);
            } catch (IOException e) {
                LOG.error("Failed to close stdin", e);
            }
        });
        final int rc = exit[0].getRc();
        final String description = exit[0].getDescription();
        final long finishTimeMillis = System.currentTimeMillis();
        LOG.info("Metric \"Time from Task start to Task finish\": {} millis", finishTimeMillis - startTimeMillis);
        LOG.info("Run:: Task finished RC = {}, Description = {}", rc, description);
        if (rc != 0) {
            System.err.print(description);
        }
        communicationLatch.await(); // waiting for slots to finish communication
        destroyChannel(stdinChannel);
        return rc;
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
        LOG.info("Creating custom slot " + slot.name());
        final String prefix = (auth.hasTask() ? auth.getTask().getTaskId() : auth.getUser().getUserId()) + ":" + pid;
        if (slot.name().startsWith("/dev/")) {
            final String devSlot = slot.name().substring("/dev/".length());
            final Map<String, String> pipeConfig = pipesConfig.get(devSlot);
            switch (devSlot) {
                case "stdin": {
                    boolean pipe = false;
                    if (!pipeConfig.getOrDefault("node", "").isEmpty()) { // linux
                        stdinChannel = prefix + ":" + devSlot + ":" + pipeConfig.get("node");
                        pipe = true;
                    } else if (!pipeConfig.getOrDefault("name", "").isEmpty()) { // macos
                        stdinChannel = prefix + ":" + devSlot + ":" + pipeConfig.get("name");
                        pipe = true;
                    } else {
                        stdinChannel = UUID.randomUUID().toString();
                    }

                    final String slotName = String.join("/", "/tasks", prefix, devSlot);
                    createChannel(slot, stdinChannel);

                    createSlotByProto(prefix + ":" + devSlot, pipe, "channel:" + stdinChannel, slotName, Slot.STDOUT);
                    final Path inputSlotFile = Path.of(lzyRoot, slotName);
                    ForkJoinPool.commonPool().execute(() -> {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (OutputStream is = Files.newOutputStream(inputSlotFile, StandardOpenOption.WRITE)) {
                            int read;
                            while (System.in.available() > 0 && (read = System.in.read(buffer)) >= 0) {
                                is.write(buffer, 0, read);
                            }
                        } catch (IOException e) {
                            LOG.warn("Unable to read from input stream", e);
                        }
                        LOG.info("Slot {} has been processed, counting down latch", devSlot);
                        communicationLatch.countDown();
                    });
                    return stdinChannel;
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
                        destroyChannel(channelName);
                        LOG.info("Slot {} has been processed, counting down latch", devSlot);
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

    private void destroyChannel(String channelName) {
        //noinspection ResultOfMethodCallIgnored
        kharon.channel(Channels.ChannelCommand.newBuilder()
            .setAuth(auth)
            .setChannelName(channelName)
            .setDestroy(Channels.ChannelDestroy.newBuilder().build())
            .build()
        );
    }

    private String createChannel(Slot slot, String channelName) {
        final Channels.ChannelStatus channel = kharon.channel(Channels.ChannelCommand.newBuilder()
            .setAuth(auth)
            .setChannelName(channelName)
            .setCreate(Channels.ChannelCreate.newBuilder()
                .setContentType(gRPCConverter.to(slot.contentType()))
                .build())
            .build()
        );
        return channel.getChannel().getChannelId();
    }
}
