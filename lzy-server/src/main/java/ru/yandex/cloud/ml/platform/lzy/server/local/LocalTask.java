package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.task.PreparingSlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class LocalTask implements Task {
    private static final Logger LOG = LogManager.getLogger(LocalTask.class);

    private final String owner;
    private final UUID tid;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final ChannelsRepository channels;
    private final URI serverURI;

    private State state = State.PREPARING;
    private List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();
    private Map<Slot, Channel> attachedSlots = new HashMap<>();
    private ManagedChannel servantChannel;
    private URI servantURI;
    private LzyServantGrpc.LzyServantBlockingStub servant;

    LocalTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        ChannelsRepository channels,
        URI serverURI
    ) {
        this.owner = owner;
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
        this.channels = channels;
        this.serverURI = serverURI;
    }

    @Override
    public UUID tid() {
        return tid;
    }

    @Override
    public Zygote workload() {
        return workload;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public void onProgress(Consumer<Servant.ExecutionProgress> listener) {
        listeners.add(listener);
    }

    @Override
    public Slot slot(String slotName) {
        return attachedSlots.keySet().stream()
            .filter(s -> s.name().equals(slotName))
            .findFirst()
            .orElse(workload.slot(slotName));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void start(String token) {
        try {
            final File taskDir = File.createTempFile("lzy", "task");
            taskDir.delete();
            taskDir.mkdirs();
            taskDir.mkdir();
            final Process process = runJvm(LzyServant.class, taskDir,
                new String[]{
                    "-z", serverURI.toString(),
                    "-p", Integer.toString(10000 + (hashCode() % 1000)),
                    "-m", taskDir.toString() + "/lzy"
                },
                Map.of(
                    "LZYTASK", tid.toString(),
                    "LZYTOKEN", token
                )
            );
            process.getOutputStream().close();
            ForkJoinPool.commonPool().execute(() -> {
                try(LineNumberReader lnr = new LineNumberReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = lnr.readLine()) != null) {
                        LOG.info(line);
                    }
                } catch (IOException e) {
                    LOG.warn("Exception in local task", e);
                }
            });
            ForkJoinPool.commonPool().execute(() -> {
                try(LineNumberReader lnr = new LineNumberReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = lnr.readLine()) != null) {
                        LOG.warn(line);
                    }
                } catch (IOException e) {
                    LOG.warn("Exception in local task", e);
                }
            });
            LOG.info("LocalTask servant exited with exit code: " + process.waitFor());
            state(State.DESTROYED);
        } catch (IOException | InterruptedException e) {
            LOG.warn("Exception in local task", e);
        }
    }

    @SuppressWarnings("WeakerAccess")
    protected void state(State newState) {
        if (newState != state) {
            state = newState;
            progress(Servant.ExecutionProgress.newBuilder()
                .setChanged(Servant.StateChanged.newBuilder().setNewState(Servant.StateChanged.State.valueOf(newState.name())).build())
                .build());
        }
    }

    @Override
    public void attachServant(URI uri) {
        servantChannel = ManagedChannelBuilder
            .forAddress(uri.getHost(), uri.getPort())
            .usePlaintext()
            .build();
        servantURI = uri;
        servant = LzyServantGrpc.newBlockingStub(servantChannel);
        final Tasks.TaskSpec.Builder builder = Tasks.TaskSpec.newBuilder()
            .setZygote(gRPCConverter.to(workload));
        assignments.forEach((slot, binding) ->
            builder.addAssignmentsBuilder()
                .setSlot(gRPCConverter.to(slot))
                .setBinding(binding)
                .build()
        );
        final Iterator<Servant.ExecutionProgress> progressIt = servant.execute(builder.build());
        state(State.CONNECTED);
        try {
            progressIt.forEachRemaining(progress -> {
                this.progress(progress);
                switch (progress.getStatusCase()) {
                    case STARTED:
                        state(State.RUNNING);
                        break;
                    case ATTACHED:
                        final Servant.AttachSlot attached = progress.getAttached();
                        final Slot slot = gRPCConverter.from(attached.getSlot());
                        final String channelName;
                        if (attached.getChannel().isEmpty()) {
                            final String binding = assignments.getOrDefault(slot, "");
                            channelName = binding.startsWith("channel:") ?
                                binding.substring("channel:".length()) :
                                null;
                        }
                        else channelName = attached.getChannel();

                        final Channel channel = channels.get(channelName);
                        if (channel != null) {
                            channels.bind(channel, new Binding(this, slot));
                            attachedSlots.put(slot, channel);
                        } else LOG.warn("Unable to attach channel to " + tid + ":" + slot.name());
                        break;
                    case EXIT:
                        state(State.FINISHED);
                        break;
                }
            });
        }
        finally {
            state(State.FINISHED);
            //noinspection ResultOfMethodCallIgnored
            servant.stop(IAM.Empty.newBuilder().build());
        }
    }

    private void progress(Servant.ExecutionProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    @Override
    public URI servant() {
        return servantURI;
    }

    @Override
    public io.grpc.Channel servantChannel() {
        return servantChannel;
    }

    @Override
    public Stream<Slot> slots() {
        return attachedSlots.keySet().stream();
    }

    @Override
    public SlotStatus slotStatus(Slot slot) throws TaskException {
        final Slot definedSlot = workload.slot(slot.name());
        if (servant == null) {
            if (definedSlot != null)
                return new PreparingSlotStatus(owner, this, definedSlot, assignments.get(slot));
            throw new TaskException("No such slot: " + tid + ":" + slot);
        }
        final Servant.SlotCommandStatus slotStatus = servant.configureSlot(Servant.SlotCommand.newBuilder()
            .setSlot(slot.name())
            .setStatus(Servant.StatusCommand.newBuilder().build())
            .build()
        );
        return gRPCConverter.from(slotStatus.getStatus());
    }

    @Override
    public void signal(TasksManager.Signal signal) throws TaskException {
        if (servant == null)
            throw new TaskException("Illegal task state: " + state());
        //noinspection ResultOfMethodCallIgnored
        servant.signal(Tasks.TaskSignal.newBuilder().setSigValue(signal.sig()).build());
    }

    @SuppressWarnings("SameParameterValue")
    private static Process runJvm(final Class<?> mainClass, File wd, final String[] args, final Map<String, String> env) {
        try {
            final Method main = mainClass.getMethod("main", String[].class);
            if (main.getReturnType().equals(void.class)
                && Modifier.isStatic(main.getModifiers())
                && Modifier.isPublic(main.getModifiers())) {
                try {
                    ProcessBuilder pb = new ProcessBuilder();
                    pb.directory(wd);
                    final List<String> parameters = pb.command();
                    parameters.add(System.getProperty("java.home") + "/bin/java");
                    parameters.add("-Xmx1g");
                    parameters.add("-classpath");
                    parameters.add(System.getProperty("java.class.path"));
                    parameters.add(mainClass.getName());
                    parameters.addAll(Arrays.asList(args));
                    pb.environment().putAll(env);
                    return pb.start();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        catch (NoSuchMethodException ignore) {}
        throw new IllegalArgumentException("Main class must contain main method :)");
    }

}
