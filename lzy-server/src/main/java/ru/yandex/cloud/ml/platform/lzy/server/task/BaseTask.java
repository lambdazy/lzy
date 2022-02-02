package ru.yandex.cloud.ml.platform.lzy.server.task;

import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.*;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;
import ru.yandex.cloud.ml.platform.lzy.server.local.ServantEndpoint;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc.LzyServantBlockingStub;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class BaseTask implements Task {
    private static final Logger LOG = LogManager.getLogger(BaseTask.class);

    protected final String owner;
    protected final UUID tid;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final ChannelsManager channels;
    protected final URI serverURI;
    @Nullable
    protected final SnapshotMeta snapshotMeta;

    private final List<Consumer<Servant.ExecutionProgress>> listeners = new ArrayList<>();
    private final Map<Slot, Channel> attachedSlots = new HashMap<>();

    private State state = State.PREPARING;
    private URI servantURI;
    private LzyServantBlockingStub servant;
    private final String bucket;
    private final CountDownLatch contextStarted = new CountDownLatch(1);

    public BaseTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        @Nullable SnapshotMeta snapshotMeta,
        ChannelsManager channels,
        URI serverURI,
        String bucket
    ) {
        this.owner = owner;
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
        this.snapshotMeta = snapshotMeta;
        this.channels = channels;
        this.serverURI = serverURI;
        this.bucket = bucket;
    }

    @Override
    public String bucket() {
        return bucket;
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
    public SnapshotMeta wbMeta() {
        return snapshotMeta;
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
        Operations.Zygote zygote = gRPCConverter.to(workload);
        final Tasks.ContextSpec.Builder contextBuilder = Tasks.ContextSpec.newBuilder()
            .setEnv(zygote.getEnv())
            .setProvisioning(zygote.getProvisioning());
        if (snapshotMeta != null){
            contextBuilder.setSnapshotMeta(SnapshotMeta.to(snapshotMeta));
        }
        assignments.forEach((slot, binding) ->
            contextBuilder.addAssignmentsBuilder()
                .setSlot(gRPCConverter.to(slot))
                .setBinding(binding)
                .build()
        );

        final Iterator<Servant.ContextProgress> contextProgress = servant.prepare(contextBuilder.build());
        LOG.info("BaseTask::attachServant called " + uri);

        final Thread contextProgressThread = new Thread(() -> contextProgress.forEachRemaining(progress -> {
            switch (progress.getStatusCase()){
                case ATTACH: {
                    LOG.info("BaseTask::attach "+ JsonUtils.printRequest(progress));
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
                    LOG.info("BaseTask::detach "+ JsonUtils.printRequest(progress));
                    final Servant.SlotDetach detach = progress.getDetach();
                    final Slot slot = gRPCConverter.from(detach.getSlot());
                    final URI slotUri = URI.create(detach.getUri());
                    final Endpoint endpoint = new ServantEndpoint(slot, slotUri, tid, servant);
                    final Channel channel = channels.bound(endpoint);
                    if (channel != null) {
                        attachedSlots.remove(slot);
                        channels.unbind(channel, endpoint);
                    }
                    break;
                }
                case START: {
                    LOG.info("Context " + uri + " started");
                    contextStarted.countDown();
                }
                case EXIT:{
                    LOG.info("Context " + uri + " exited");
                    state(State.FINISHED);
                }
            }
        }));
        contextProgressThread.start();

        final Tasks.TaskSpec.Builder builder = Tasks.TaskSpec.newBuilder()
            .setAuth(IAM.Auth.newBuilder()
                .setTask(IAM.TaskCredentials.newBuilder()
                    .setTaskId(tid.toString())
                    .build())
                .build())
            .setZygote(gRPCConverter.to(workload));
        if (snapshotMeta != null) {
            builder.setSnapshotMeta(SnapshotMeta.to(snapshotMeta));
        }
        assignments.forEach((slot, binding) ->
            builder.addAssignmentsBuilder()
                .setSlot(gRPCConverter.to(slot))
                .setBinding(binding)
                .build()
        );

        try {
            contextStarted.await();
        } catch (InterruptedException e) {
            LOG.error(e);
        }
        final Iterator<Servant.ExecutionProgress> progressIt = servant.execute(builder.build());
        state(State.CONNECTED);
        LOG.info("Server is attached to servant {}", servantURI);
        try {
            progressIt.forEachRemaining(progress -> {
                LOG.info("BaseTask::Progress " + JsonUtils.printRequest(progress));
                this.progress(progress);
                switch (progress.getStatusCase()) {
                    case STARTED:
                        state(State.RUNNING);
                        break;
                    case EXIT:
                        break;
                }
            });
            contextProgressThread.join();
        } catch (InterruptedException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        } finally {
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
}
