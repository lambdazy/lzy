package ai.lzy.servant.commands;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.logs.MetricEvent;
import ai.lzy.logs.MetricEventLogger;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import ai.lzy.v1.deprecated.LzyServerGrpc;
import ai.lzy.v1.deprecated.LzyTask;
import ai.lzy.v1.deprecated.LzyZygote;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
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
    private final CountDownLatch communicationLatch = new CountDownLatch(2);
    private final List<ChannelDescription> channels = new ArrayList<>();
    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManager;
    private LzyAuth.Auth auth;
    private Map<String, Map<String, String>> pipesConfig;
    private LzyFsGrpc.LzyFsBlockingStub servantFs;
    private String agentId;
    private String lzyRoot;

    private record ChannelDescription(
        String channelName,
        String channelId
    ) {}

    @Override
    public int execute(CommandLine command) throws Exception {
        final CommandLine localCmd = parse(command, options);
        if (localCmd == null) {
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
        agentId = command.getOptionValue("agent-id");
        pipesConfig = pipesConfig();

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final URI channelManagerAddr = URI.create(command.getOptionValue("channel-manager"));
        auth = LzyAuth.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        final ManagedChannel servant = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('q')))
            .usePlaintext()
            .enableRetry(LzyFsGrpc.SERVICE_NAME)
            .build();
        this.servantFs = LzyFsGrpc.newBlockingStub(servant);
        final ManagedChannel channelManagerChannel = ChannelBuilder
            .forAddress(channelManagerAddr.getHost(), channelManagerAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        channelManager = LzyChannelManagerPrivateGrpc
            .newBlockingStub(channelManagerChannel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION,
                auth.getUser()::getToken
            ));

        final LzyZygote.Zygote.Builder builder = LzyZygote.Zygote.newBuilder();
        JsonFormat.parser().merge(System.getenv("ZYGOTE"), builder);
        final LzyZygote.Zygote grpcZygote = builder.build();
        final Zygote zygote = GrpcConverter.from(grpcZygote);
        final LzyTask.TaskSpec.Builder taskSpecBuilder = LzyTask.TaskSpec.newBuilder();
        taskSpecBuilder.setAuth(auth);
        taskSpecBuilder.setZygote(grpcZygote);
        zygote.slots().forEach(slot -> {
            LOG.info("Resolving slot " + slot.name());
            final String binding;
            if (slot.media() == Slot.Media.ARG) {
                binding = String.join(" ", command.getArgList().subList(1, command.getArgList().size()));
            } else if (bindings.containsKey(slot.name())) {
                binding = bindings.get(slot.name());
            } else {
                binding = resolveChannel(slot);
            }
            LOG.info("Slot " + slot.name() + " resolved to " + binding);
            taskSpecBuilder.addAssignmentsBuilder()
                .setSlot(ProtoConverter.toProto(slot))
                .setBinding(binding)
                .build();
        });

        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);
        final long startTimeMillis = System.currentTimeMillis();
        final LzyTask.TaskSpec taskSpec = taskSpecBuilder.build();
        LOG.info("Running taskSpec: {}", JsonUtils.printRequest(taskSpec));
        final Iterator<LzyTask.TaskProgress> executionProgress = server.start(taskSpec);
        final int[] exit = new int[] {-1};
        final String[] descriptionArr = new String[] {"Got no exit code"};
        executionProgress.forEachRemaining(progress -> {
            try {
                LOG.info("[progress] " + JsonFormat.printer().print(progress));
                if (progress.getStatus() == LzyTask.TaskProgress.Status.ERROR
                    || progress.getStatus() == LzyTask.TaskProgress.Status.SUCCESS)
                {
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
        channels.forEach(channel -> {
            try {
                destroyChannel(channel);
            } catch (Exception e) {
                System.err.println("Can't destroy channel " + channel + ": " + e.getMessage());
                e.printStackTrace(System.err);
            }
        });
        return rc;
    }

    private Map<String, Map<String, String>> pipesConfig() throws IOException {
        final long pid = ProcessHandle.current().pid();
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
            (auth.hasTask() ? auth.getTask().getTaskId() : auth.getUser().getUserId()) + ":" + agentId;
        if (slot.name().startsWith("/dev/")) {
            final String devSlot = slot.name().substring("/dev/".length());
            final Map<String, String> pipeConfig = pipesConfig.get(devSlot);
            switch (devSlot) {
                case "stdin" -> {
                    throw new RuntimeException("Slot stdin is not supported.");
                }
                case "stdout", "stderr" -> {
                    final String channelName;
                    if (!pipeConfig.getOrDefault("node", "").isEmpty()) { // linux
                        channelName = prefix + ":" + devSlot + ":" + pipeConfig.get("node");
                    } else if (pipeConfig.getOrDefault("device", "").startsWith("0x")) { // macos
                        channelName = prefix + ":" + devSlot + ":" + pipeConfig.get("name");
                    } else {
                        channelName = UUID.randomUUID().toString();
                    }

                    final String slotName = String.join("/", "/tasks", prefix, devSlot);
                    final String channelId = createChannel(slot, channelName);
                    final ChannelDescription channelDescription = new ChannelDescription(channelName, channelId);
                    channels.add(channelDescription);

                    createSlotByProto(channelId, slotName, Slot.STDIN);

                    final Path outputSlotFile = Path.of(lzyRoot, slotName);
                    ForkJoinPool.commonPool().execute(() -> {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (InputStream is = Files.newInputStream(outputSlotFile, StandardOpenOption.READ)) {
                            int read;
                            while ((read = is.read(buffer)) >= 0) {
                                if (read > 0) {
                                    ("stderr".equals(devSlot) ? System.err : System.out).write(buffer, 0, read);
                                }
                            }
                        } catch (IOException e) {
                            LOG.warn("Unable to read from " + devSlot, e);
                        } finally {
                            destroyChannel(channelDescription);
                            LOG.info("Slot {} has been processed, counting down latch", devSlot);
                            communicationLatch.countDown();
                        }
                    });
                    return channelId;
                }
                default -> throw new IllegalArgumentException(
                    MessageFormat.format("Illegal slot found: {0}", slot.name())
                );
            }
        } else {
            throw new IllegalArgumentException(
                MessageFormat.format("Slot {0} assignment is not specified", slot.name())
            );
        }
    }

    private void createSlotByProto(String channelId, String slotName, Slot slotProto) {
        LOG.info("Create slot `{}` for channel `{}` with taskId {}.", slotName, channelId, agentId);
        try {
            final LMS.Slot slotDeclaration = LMS.Slot.newBuilder(ProtoConverter.toProto(slotProto))
                .setName(slotName)
                .build();
            var ret = servantFs.createSlot(LzyFsApi.CreateSlotRequest.newBuilder()
                .setTaskId(agentId)
                .setSlot(slotDeclaration)
                .setChannelId(channelId)
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

    private void destroyChannel(ChannelDescription channelDescription) {
        try {
            final LCMPS.ChannelDestroyResponse destroyResponse =
                channelManager.destroy(LCMPS.ChannelDestroyRequest.newBuilder()
                    .setChannelId(channelDescription.channelId())
                    .build()
                );
            LOG.info(destroyResponse);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                return;
            }
            throw e;
        }
    }

    private String createChannel(Slot slot, String channelName) {
        LOG.info("Create channel `{}` for slot `{}`.", channelName, slot.name());
        final LCMPS.ChannelCreateResponse channelCreateResponse = channelManager.create(
            LCMPS.ChannelCreateRequest.newBuilder()
                .setChannelSpec(LCM.ChannelSpec.newBuilder()
                    .setChannelName(channelName)
                    .setContentType(ProtoConverter.toProto(slot.contentType()))
                    .setDirect(LCM.DirectChannelType.newBuilder().build())
                    .build())
                .setExecutionId(agentId)
                .build()
        );
        return channelCreateResponse.getChannelId();
    }
}
