package ru.yandex.cloud.ml.platform.lzy.server.local;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import ru.yandex.cloud.ml.platform.lzy.server.task.PreparingSlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class BaseTask implements Task {
    private static final Logger LOG = LogManager.getLogger(BaseTask.class);

    private final String owner;
    private final UUID tid;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final ChannelsManager channels;
    private final URI serverURI;

    private final List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();
    private final Map<Slot, Channel> attachedSlots = new HashMap<>();

    private State state = State.PREPARING;
    private ManagedChannel servantChannel;
    private URI servantURI;
    private LzyServantBlockingStub servant;

    BaseTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        ChannelsManager channels,
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

    @Override
    public void start(String token) {
        final int port = (10000 + (hashCode() % 1000));
        runServantAndWaitFor(serverURI.getHost(), serverURI.getPort(), "localhost", port, tid, token);
        LOG.info("LocalTask servant exited");
        state(State.DESTROYED);
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
    public void attachServant(URI uri, LzyServantBlockingStub servant) {
        servantURI = uri;
        this.servant = servant;
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
        LOG.info("Server is attached to servant {}", servantURI);
        try {
            progressIt.forEachRemaining(progress -> {
                LOG.info("LocalTask::Progress " + JsonUtils.printRequest(progress));
                this.progress(progress);
                switch (progress.getStatusCase()) {
                    case STARTED:
                        state(State.RUNNING);
                        break;
                    case ATTACH: {
                        final Servant.SlotAttach attach = progress.getAttach();
                        final Slot slot = gRPCConverter.from(attach.getSlot());
                        final URI slotUri = URI.create(attach.getUri());
                        final String channelName;
                        if (attach.getChannel().isEmpty()) {
                            final String binding = assignments.getOrDefault(slot, "");
                            channelName = binding.startsWith("channel:") ?
                                binding.substring("channel:".length()) :
                                null;
                        }
                        else channelName = attach.getChannel();

                        final Channel channel = channels.get(channelName);
                        if (channel != null) {
                            attachedSlots.put(slot, channel);
                            channels.bind(channel, new ServantEndpoint(slot, slotUri, tid, servant));
                        } else {
                            LOG.warn("Unable to attach channel to " + tid + ":" + slot.name());
                        }
                        break;
                    }
                    case DETACH: {
                        final Servant.SlotDetach detach = progress.getDetach();
                        final Slot slot = gRPCConverter.from(detach.getSlot());
                        final URI slotUri = URI.create(detach.getUri());
                        final String linkToStorage = detach.getLinkToStorage();
                        final Endpoint endpoint = new ServantEndpoint(slot, slotUri, tid, servant);
                        final Channel channel = channels.bound(endpoint);
                        if (channel != null) {
                            attachedSlots.remove(slot);
                            channels.unbind(channel, endpoint);
                            if (!linkToStorage.equals("")) {
                                channels.addLinkToStorage(channel, slot.name(), linkToStorage);
                            }
                        }
                        break;
                    }
                    case EXIT:
                        state(State.FINISHED);
                        break;
                }
            });
        }
        finally {
            state(State.FINISHED);
            LOG.info("Stopping servant {}", servantURI);
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
    protected abstract void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort, UUID tid, String token);
}
