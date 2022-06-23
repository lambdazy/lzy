package ai.lzy.servant.commands;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
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
        options.addOption(new Option("n", "name", true, "Task name"));
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CountDownLatch communicationLatch = new CountDownLatch(3);
    private final List<String> channels = new ArrayList<>();
    private LzyServerGrpc.LzyServerBlockingStub server;
    private IAM.Auth auth;
    private Map<String, Map<String, String>> pipesConfig;
    private LzyFsGrpc.LzyFsBlockingStub servantFs;
    private long pid;
    private String lzyRoot;

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
            LOG.info("Bindings: " + bindings.entrySet().stream()
                .map(e -> e.getKey() + " -> " + e.getValue())
                .collect(Collectors.joining(";\n"))
            );
        }

        lzyRoot = command.getOptionValue('m');
        pid = ProcessHandle.current().pid();
        pipesConfig = pipesConfig();

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        if (pid != 0 /* always true */) {
            final ManagedChannel serverCh = ChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                .build();
            server = LzyServerGrpc.newBlockingStub(serverCh);
        }

        if (pid != 0 /* always true */) {
            final ManagedChannel servant = ChannelBuilder
                .forAddress("localhost", Integer.parseInt(command.getOptionValue('q')))
                .usePlaintext()
                .enableRetry(LzyFsGrpc.SERVICE_NAME)
                .build();
            this.servantFs = LzyFsGrpc.newBlockingStub(servant);
        }

        final Operations.Zygote.Builder builder = Operations.Zygote.newBuilder();
        JsonFormat.parser().merge(System.getenv("ZYGOTE"), builder);
        builder.setName(builder.getName() + "_" + pid);
        final Operations.Zygote grpcZygote = builder.build();
        final Zygote zygote = GrpcConverter.from(grpcZygote);
        final Tasks.TaskSpec.Builder taskSpec = Tasks.TaskSpec.newBuilder();
        taskSpec.setAuth(auth);
        taskSpec.setZygote(grpcZygote);
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
            LOG.info("Slot " + slot.name() + " resolved to " + binding);
            taskSpec.addAssignmentsBuilder()
                .setSlot(GrpcConverter.to(slot))
                .setBinding(binding)
                .build();
        });

        final long startTimeMillis = System.currentTimeMillis();
        final Iterator<Tasks.TaskProgress> executionProgress = server.start(taskSpec.build());
        final int[] exit = new int[] {-1};
        final String[] descriptionArr = new String[] {"Got no exit code"};
        executionProgress.forEachRemaining(progress -> {
            try {
                LOG.info(JsonFormat.printer().print(progress));
                if (progress.getStatus() == Tasks.TaskProgress.Status.ERROR
                    || progress.getStatus() == Tasks.TaskProgress.Status.SUCCESS) {
                    exit[0] = progress.getRc();
                    descriptionArr[0] = progress.getDescription();
                    System.in.close();
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.warn("Unable to parse execution progress", e);
            } catch (IOException e) {
                LOG.error("Unable to close stdin", e);
            }
        });
        final int rc = exit[0];
        final String description = descriptionArr[0];
        final long finishTimeMillis = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from Task start to Task finish",
                Map.of("metric_type", "task_metric"),
                finishTimeMillis - startTimeMillis
            )
        );
        LOG.info("Run:: Task finished RC = {}, Description = {}", rc, description);
        if (rc != 0) {
            System.err.print(description);
        } else {
            communicationLatch.await(); // waiting for slots to finish communication
        }
        channels.forEach(this::destroyChannel);
        return rc;
    }

    private Map<String, Map<String, String>> pipesConfig() throws IOException {
        final Process p = Runtime.getRuntime().exec("lsof -p " + pid + " -a -d0,1,2 -F ftidn");
        final InputStreamReader inputStreamReader = new InputStreamReader(
            p.getInputStream(),
            StandardCharsets.UTF_8
        );
        String[] fdNames = new String[] {"stdin", "stdout", "stderr"};
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
                        pipeMappings.computeIfAbsent(name, n -> new HashMap<>())
                            .put(namesMappings.getOrDefault(line.charAt(0), "unknown"),
                                line.substring(1));
                        break;
                    default:
                        pipeMappings.computeIfAbsent(name, n -> new HashMap<>())
                            .put(namesMappings.getOrDefault(line.charAt(0), "unknown"),
                                line.substring(1));
                        break;
                }
            }
        }
        return pipeMappings;
    }

    private String resolveChannel(Slot slot) {
        LOG.info("Creating custom slot " + slot.name());
        final String prefix =
            (auth.hasTask() ? auth.getTask().getTaskId() : auth.getUser().getUserId()) + ":" + pid;
        if (slot.name().startsWith("/dev/")) {
            final String devSlot = slot.name().substring("/dev/".length());
            final Map<String, String> pipeConfig = pipesConfig.get(devSlot);
            switch (devSlot) {
                case "stdin": {
                    boolean pipe = false;
                    final String stdinChannel;
                    if (!pipeConfig.getOrDefault("node", "").isEmpty()) { // linux
                        stdinChannel = prefix + ":" + devSlot + ":" + pipeConfig.get("node");
                        pipe = true;
                    } else if (!pipeConfig.getOrDefault("name", "").isEmpty()) { // macos
                        stdinChannel = prefix + ":" + devSlot + ":" + pipeConfig.get("name");
                        pipe = true;
                    } else {
                        stdinChannel = "stdin_" + UUID.randomUUID();
                    }
                    channels.add(stdinChannel);

                    final String slotName = String.join("/", "/tasks", prefix, devSlot);
                    createChannel(slot, stdinChannel);

                    createSlotByProto(pid, prefix + ":" + devSlot, pipe, "channel:" + stdinChannel,
                        slotName, Slot.STDOUT);
                    final Path inputSlotFile = Path.of(lzyRoot, slotName);
                    ForkJoinPool.commonPool().execute(() -> {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (OutputStream is = Files
                            .newOutputStream(inputSlotFile, StandardOpenOption.WRITE)) {
                            int read;
                            while (System.in.available() > 0
                                && (read = System.in.read(buffer)) >= 0) {
                                is.write(buffer, 0, read);
                            }
                        } catch (IOException e) {
                            LOG.warn("Unable to read from input stream", e);
                        } finally {
                            LOG.info("Slot {} has been processed, counting down latch", devSlot);
                            communicationLatch.countDown();
                        }
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
                    channels.add(channelName);

                    final String slotName = String.join("/", "/tasks", prefix, devSlot);
                    final String channelId = createChannel(slot, channelName);

                    createSlotByProto(pid, prefix + ":" + devSlot, pipe, channelId, slotName, Slot.STDIN);

                    final Path outputSlotFile = Path.of(lzyRoot, slotName);
                    ForkJoinPool.commonPool().execute(() -> {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (InputStream is = Files
                            .newInputStream(outputSlotFile, StandardOpenOption.READ)) {
                            int read;
                            while ((read = is.read(buffer)) >= 0) {
                                ("stderr".equals(devSlot) ? System.err : System.out)
                                    .write(buffer, 0, read);
                            }
                        } catch (IOException e) {
                            LOG.warn("Unable to read from " + devSlot, e);
                        } finally {
                            destroyChannel(channelName);
                            LOG.info("Slot {} has been processed, counting down latch", devSlot);
                            communicationLatch.countDown();
                        }
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

    private void createSlotByProto(long pid, String name, boolean pipe, String channelId,
                                   String slotName, Slot slotProto) {
        LOG.info("Create {}slot `{}` ({}) for channel `{}` with taskId {}.",
            pipe ? "pipe " : "", slotName, name, channelId, pid);
        try {
            final Operations.Slot slotDeclaration = Operations.Slot.newBuilder(GrpcConverter.to(slotProto))
                .setName(slotName)
                .build();
            var ret = servantFs.configureSlot(LzyFsApi.SlotCommand.newBuilder()
                .setSlot(name)
                .setTid(Long.toString(pid))
                .setCreate(LzyFsApi.CreateSlotCommand.newBuilder()
                    .setSlot(slotDeclaration)
                    .setIsPipe(pipe)
                    .setChannelId(channelId)
                    .build()
                )
                .build()
            );
            if (ret.getRc().getCode() != LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS) {
                throw new RuntimeException("Can't create slot " + slotName + ": " + ret);
            }
        } catch (Exception e) {
            LOG.error("Unable to create slot: " + slotName, e);
            throw e;
        }
    }

    private void destroyChannel(String channelName) {
        try {
            //noinspection ResultOfMethodCallIgnored
            server.channel(Channels.ChannelCommand.newBuilder()
                .setAuth(auth)
                .setChannelName(channelName)
                .setDestroy(Channels.ChannelDestroy.newBuilder().build())
                .build()
            );
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.NOT_FOUND) {
                return;
            }
            throw e;
        }

    }

    private String createChannel(Slot slot, String channelName) {
        LOG.info("Create channel `{}` for slot `{}`.", channelName, slot.name());
        final Channels.ChannelStatus channel = server.channel(Channels.ChannelCommand.newBuilder()
            .setAuth(auth)
            .setChannelName(channelName)
            .setCreate(Channels.ChannelCreate.newBuilder()
                .setContentType(GrpcConverter.to(slot.contentType()))
                .setDirect(Channels.DirectChannelSpec.newBuilder().build())
                .build())
            .build()
        );
        return channel.getChannel().getChannelId();
    }
}
