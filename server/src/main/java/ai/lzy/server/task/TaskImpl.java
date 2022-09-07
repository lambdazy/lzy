package ai.lzy.server.task;

import static ai.lzy.server.task.Task.State.ERROR;
import static ai.lzy.server.task.Task.State.EXECUTING;
import static ai.lzy.server.task.Task.State.SUCCESS;

import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Signal;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.basic.SlotStatus;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.v1.LzyFsApi;
import ai.lzy.v1.LzyFsGrpc;
import ai.lzy.v1.Operations;
import ai.lzy.v1.Servant.ExecutionConcluded;
import ai.lzy.v1.Tasks;
import ai.lzy.server.ServantsAllocator;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

public class TaskImpl implements Task {

    private static final Logger LOG = LogManager.getLogger(TaskImpl.class);

    protected final String owner;
    protected final String tid;
    protected final URI serverURI;
    private final Zygote workload;
    private final Map<Slot, String> assignments;
    private final List<Consumer<Tasks.TaskProgress>> listeners = Collections.synchronizedList(new ArrayList<>());
    private ServantsAllocator.ServantConnection servant;
    private State state = State.PREPARING;
    private final List<Signal> signalsQueue = new ArrayList<>();

    public TaskImpl(String owner, String tid, Zygote workload, Map<Slot, String> assignments, URI serverURI) {
        this.owner = owner;
        this.tid = tid;
        this.workload = workload;
        this.assignments = assignments;
        this.serverURI = serverURI;
    }

    @Override
    public String tid() {
        return tid;
    }

    @Override
    public String workloadName() {
        return workload.name();
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
    @SuppressWarnings("WeakerAccess")
    public synchronized void state(State newState, int rc, String... description) {
        if (newState != state) {
            state = newState;
            progress(Tasks.TaskProgress.newBuilder()
                .setTid(tid)
                .setZygoteName(workloadName())
                .setStatus(Tasks.TaskProgress.Status.valueOf(newState.name()))
                .setDescription(String.join("\n", description))
                .setRc(rc)
                .build());
        }
    }

    @Override
    public synchronized void state(State newState, String... description) {
        if (newState != state) {
            state = newState;
            progress(Tasks.TaskProgress.newBuilder()
                .setTid(tid)
                .setZygoteName(workloadName())
                .setStatus(Tasks.TaskProgress.Status.valueOf(newState.name()))
                .setDescription(String.join("\n", description))
                .build());
        }
    }

    @Override
    public void onProgress(Consumer<Tasks.TaskProgress> listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized void attachServant(ServantsAllocator.ServantConnection connection) {
        LOG.info("Server is attached to servant {}", connection.uri());

        connection.onProgress(progress -> {
            synchronized (TaskImpl.this) {
                switch (progress.getStatusCase()) {
                    case EXECUTESTART -> {
                        LOG.info("Task " + tid + " started");
                        state(State.EXECUTING);
                        signalsQueue.forEach(s -> {
                            //noinspection ResultOfMethodCallIgnored
                            servant.control().signal(Tasks.TaskSignal.newBuilder().setSigValue(s.sig()).build());
                        });
                        return true;
                    }
                    case EXECUTESTOP -> {
                        final ExecutionConcluded executeStop = progress.getExecuteStop();
                        LOG.info("Task " + tid + " exited rc: " + executeStop.getRc());
                        if (executeStop.getRc() != 0) {
                            state(ERROR, executeStop.getRc(), "Exit code: " + executeStop.getRc(),
                                executeStop.getDescription());
                        } else {
                            state(State.SUCCESS, 0, "Success");
                        }
                        servant = null;
                        TaskImpl.this.notifyAll();
                        return true; //clean up listener on CONCLUDED state
                    }
                    case COMMUNICATIONCOMPLETED -> {
                        return state.phase() <= EXECUTING.phase();
                    }
                    case FAILED -> {
                        state(ERROR, ReturnCodes.INTERNAL_ERROR.getRc(), "Internal error");
                        return false;
                    }
                    case CONCLUDED -> {
                        if (!EnumSet.of(ERROR, State.SUCCESS).contains(state)) {
                            state(ERROR, ReturnCodes.INTERNAL_ERROR.getRc(), "Connection error");
                        }
                        return false;
                    }
                    default -> {
                        return true;
                    }
                }
            }
        });
        this.servant = connection;
        TaskImpl.this.notifyAll();
        final Tasks.TaskSpec.Builder taskSpecBuilder = Tasks.TaskSpec.newBuilder();
        taskSpecBuilder.setTid(tid);
        taskSpecBuilder.setZygote(GrpcConverter.to(workload));
        assignments.forEach((slot, binding) -> {
            // need to filter out std* slots because they don't exist on prepare
            taskSpecBuilder.addAssignmentsBuilder()
                .setSlot(ProtoConverter.toProto(slot))
                .setBinding(binding)
                .build();
        });
        //noinspection ResultOfMethodCallIgnored
        connection.control().execute(taskSpecBuilder.build());
    }

    private void progress(Tasks.TaskProgress progress) {
        listeners.forEach(l -> l.accept(progress));
    }

    @Override
    public synchronized URI servantUri() {
        return servant != null ? servant.uri() : null;
    }

    @Nullable
    @Override
    public synchronized URI servantFsUri() {
        return servant != null ? servant.fsUri() : null;
    }

    @Override
    public SlotStatus slotStatus(Slot slot) throws TaskException {
        final LzyFsGrpc.LzyFsBlockingStub fs;
        synchronized (this) {
            final Slot definedSlot = workload.slot(slot.name());
            if (servant == null) {
                if (definedSlot != null) {
                    return new PreparingSlotStatus(owner, this, definedSlot, assignments.get(slot));
                }
                throw new TaskException("No such slot: " + tid + ":" + slot);
            }
            fs = servant.fs();
        }
        final LzyFsApi.SlotCommandStatus slotStatus = fs.statusSlot(
            LzyFsApi.StatusSlotRequest.newBuilder()
                .setSlotInstance(
                    LzyFsApi.SlotInstance.newBuilder()
                        .setTaskId(tid)
                        .setSlot(
                            Operations.Slot.newBuilder()
                                .setName(slot.name())
                                .build())
                        .build()
                ).build());
        return ProtoConverter.fromProto(slotStatus.getStatus());
    }

    @Override
    public synchronized void signal(Signal signal) throws TaskException {
        if (EnumSet.of(ERROR, SUCCESS).contains(state())) {
            throw new TaskException("Task is already concluded");
        }
        signalsQueue.add(signal);
        if (servant != null) {
            LOG.info("Sending signal {} to servant {} for task {}", signal.name(), servant.uri(), tid);
            //noinspection ResultOfMethodCallIgnored
            servant.control().signal(Tasks.TaskSignal.newBuilder().setSigValue(signal.sig()).build());
        } else {
            LOG.info("Postponing signal {} for task {}", signal.name(), tid);
        }
    }
}
