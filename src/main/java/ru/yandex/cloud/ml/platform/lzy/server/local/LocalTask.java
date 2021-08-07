package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.servant.LzyServant;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.task.PreparingSlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class LocalTask implements Task {
    private static final Logger LOG = LogManager.getLogger(LocalTask.class);

    private static ExecutorService pool = ForkJoinPool.commonPool();
    public final UUID tid;
    private final Zygote workload;
    private final Map<Slot, Channel> assignments;
    private final ChannelsRepository channels;

    private State state = State.PREPARING;
    private List<Consumer<State>> listeners = new ArrayList<>();
    private Map<Slot, Channel> attachedSlots = new HashMap<>();
    private ManagedChannel servantChannel;
    private URI servantURI;
    private LzyServantGrpc.LzyServantBlockingStub servant;

    LocalTask(UUID tid, Zygote workload, Map<Slot, Channel> assignments, ChannelsRepository channels) {
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
        this.channels = channels;
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
    public void onStateChange(Consumer<State> listener) {
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
            runJvm(LzyServant.class, taskDir, "-token", token);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void state(State newState) {
        if (newState != state) {
            state = newState;
            listeners.forEach(c -> c.accept(state));
        }
    }

    @Override
    public void attachServant(URI uri) {
        servantChannel = ManagedChannelBuilder
            .forAddress(uri.getHost(), uri.getPort())
            .build();
        servantURI = uri;
        servant = LzyServantGrpc.newBlockingStub(servantChannel);
        final Servant.ExecutionSpec.Builder builder = Servant.ExecutionSpec.newBuilder()
            .setDefinition(gRPCConverter.to(workload));
        assignments.forEach((slot, channel) ->
            builder.addSlotsBuilder()
                .setSlot(gRPCConverter.to(slot))
                .setChannelId(channel.name())
                .build()
        );
        final Iterator<Servant.ExecutionProgress> progressIt = servant.execute(builder.build());
        state(State.CONNECTED);
        pool.execute(() -> {
            try {
                progressIt.forEachRemaining(progress -> {
                    switch (progress.getStatusCase()) {
                        case STARTED:
                            state(State.RUNNING);
                            break;
                        case ATTACHED:
                            final Servant.AttachSlot attached = progress.getAttached();
                            final Slot slot = gRPCConverter.from(attached.getSlot());
                            final String channelName = attached.getChannel().isEmpty() ? null : attached.getChannel();
                            final Channel channel =
                                channelName != null ? channels.get(channelName) : assignments.get(slot);
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
            }
        });
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
                return new PreparingSlotStatus(this, definedSlot, assignments.get(slot).name());
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

    @SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
    private static Process runJvm(final Class<?> mainClass, File wd, final String... args) {
        try {
            final Method main = mainClass.getMethod("main", String[].class);
            if (main.getReturnType().equals(void.class)
                && Modifier.isStatic(main.getModifiers())
                && Modifier.isPublic(main.getModifiers())) {
                try {
                    final List<String> parameters = new ArrayList<>();
                    parameters.add(System.getProperty("java.home") + "/bin/java");
                    parameters.add("-Xmx1g");
                    parameters.add("-classpath");
                    parameters.add(System.getProperty("java.class.path"));
                    parameters.add(mainClass.getName());
                    parameters.addAll(Arrays.asList(args));
                    return Runtime.getRuntime().exec(
                        parameters.toArray(new String[parameters.size()]),
                        new String[0],
                        wd
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }
        catch (NoSuchMethodException ignore) { }
        throw new IllegalArgumentException("Main class must contain main method :)");
    }

}
