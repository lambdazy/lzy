package ai.lzy.scheduler.exp;

import ai.lzy.model.graph.Provisioning;
import com.google.common.net.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class ServantAllocatorExpBase implements ServantAllocatorExp.Ex {
    private static final Logger LOG = LogManager.getLogger(ServantAllocatorExpBase.class);

    protected record ServantSpec(
        String userId,
        String executionId,
        String servantId,
        Provisioning provisioning
    ) {}

    protected record PendingServant(
        ServantSpec servantSpec,
        String ott,
        Instant allocationDeadline,
        CompletableFuture<AllocatedServant> result
    ) {}

    protected static class RunningServantState {
        private final ServantSpec servantSpec;
        private final HostAndPort servantApiAddress;
        private final HostAndPort servantFsApiAddress;
        private final Map<String, String> meta;
        private volatile Instant lastActivityTime = Instant.now();

        RunningServantState(ServantSpec servantSpec, HostAndPort servantApiAddress, HostAndPort servantFsApiAddress,
                            Map<String, String> meta) {
            this.servantSpec = servantSpec;
            this.servantApiAddress = servantApiAddress;
            this.servantFsApiAddress = servantFsApiAddress;
            this.meta = meta;
        }
    }

    protected static class ExecutionState {
        private final String executionId;
        private final Map<String, RunningServantState> runningServants = new ConcurrentHashMap<>();

        ExecutionState(String executionId) {
            this.executionId = executionId;
        }
    }

    protected static class UserState {
        private final String userId;
        private final Map<String, ExecutionState> executions = new ConcurrentHashMap<>();

        UserState(String userId) {
            this.userId = userId;
        }
    }

    protected final Map<String, UserState> users = new ConcurrentHashMap<>();
    protected final Map<String, PendingServant> pendingServants = new ConcurrentHashMap<>();
    private final Timer pendingServantsTimer = new Timer("pending-servants-timer", true);
    private final Timer runningServantsTimer = new Timer("running-servants-timer", true);

    protected ServantAllocatorExpBase() {
        pendingServantsTimer.schedule(new CancelOutdatedRequests(), Duration.ofSeconds(10).toMillis());
        pendingServantsTimer.schedule(new DeallocateOutdatedServants(), Duration.ofMinutes(2).toMillis());
    }

    @Override
    public AllocatedServant allocate(String userId, String executionId, Provisioning provisioning, Duration timeout)
            throws TimeoutException, ServantAllocationException {

        var servantId = "servant_" + UUID.randomUUID();
        var servantSpec = new ServantSpec(userId, executionId, servantId, provisioning);
        var allocationDeadline = timeout.isZero() ? Instant.MAX : Instant.now().plus(timeout);

        LOG.info("Allocation request, spec={}, timeout={}", servantSpec, timeout);

        var userState = users.computeIfAbsent(userId, UserState::new);
        var executionState = userState.executions.computeIfAbsent(executionId, ExecutionState::new);

        var pendingServant = new PendingServant(servantSpec, UUID.randomUUID().toString(), allocationDeadline,
            new CompletableFuture<>());

        pendingServants.put(servantId, pendingServant);
        try {
            requestAllocation(servantSpec);
        } catch (ServantAllocationException e) {
            LOG.error("Exception while allocation request for servant {}: {}", servantId, e.getMessage(), e);
            pendingServants.remove(servantId);
            throw e;
        }

        try {
            return pendingServant.result().get();
        } catch (InterruptedException e) {
            throw new ServantAllocationException("", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException te) {
                throw te;
            }
            throw new ServantAllocationException("", e);
        }
    }

    @Override
    public void register(String servantId, String ott, HostAndPort apiEndpoint, HostAndPort fsApiEndpoint,
                         Map<String, String> meta) {
        LOG.info("Register servant {}", servantId);

        var pendingServant = pendingServants.remove(servantId);
        if (pendingServant == null) {
            LOG.warn("Too late register request, servant {} allocation has been cancelled", servantId);
            deallocate(servantId);
            return;
        }

        if (!pendingServant.ott.equals(ott)) {
            LOG.error("Servant {} token mismatch!", servantId);
            deallocate(servantId);
            pendingServant.result.completeExceptionally(new ServantAllocationException("Internal error"));
            return;
        }

        var userState = users.get(pendingServant.servantSpec().userId());
        var executionState = userState.executions.get(pendingServant.servantSpec().executionId());

        var runningServant = new RunningServantState(pendingServant.servantSpec(), apiEndpoint, fsApiEndpoint, meta);
        executionState.runningServants.put(servantId, runningServant);

        LOG.info("Servant {} successfully registered", servantId);
        pendingServant.result.complete(new AllocatedServant(servantId, apiEndpoint, fsApiEndpoint, meta));
    }

    @Override
    public void report(String servantId, ServantState state) {
        for (var userState : users.values()) {
            for (var executionState : userState.executions.values()) {
                var runningServant = executionState.runningServants.get(servantId);
                if (runningServant == null) {
                    LOG.error("Too late (?) report message '{}' from servant '{}'", state, servantId);
                    return;
                }

                runningServant.lastActivityTime = Instant.now();

                if (state == ServantState.Finish) {
                    LOG.info("Got 'Finish' from servant '{}', deallocate it", servantId);
                    executionState.runningServants.remove(servantId);
                    deallocate(servantId);
                    return;
                }
            }
        }
    }

    protected abstract void requestAllocation(ServantSpec servantSpec) throws ServantAllocationException;

    protected abstract void cancelAllocation(ServantSpec servantSpec);

    protected abstract void deallocate(String servantId);


    private final class CancelOutdatedRequests extends TimerTask {
        @Override
        public void run() {
            var now = Instant.now();

            pendingServants.forEach((servantId, servant) -> {
                if (servant.allocationDeadline.isBefore(now)) {
                    var pendingServant = pendingServants.remove(servantId);
                    if (pendingServant != null) {
                        LOG.warn("Cancel outdated request for servant {}", servantId);
                        pendingServant.result.completeExceptionally(new TimeoutException());
                        cancelAllocation(pendingServant.servantSpec);
                    }
                }
            });

            pendingServantsTimer.schedule(this, Duration.ofSeconds(10).toMillis());
        }
    }

    private final class DeallocateOutdatedServants extends TimerTask {
        private static final Duration MAX_INACTIVE_TIME = Duration.ofMinutes(10);
        private static final Duration WARN_INACTIVE_TIME = Duration.ofMinutes(4);

        @Override
        public void run() {
            var now = Instant.now();
            var maxLastActivityTime = now.minus(MAX_INACTIVE_TIME);
            var warnLastActivityTime = now.minus(WARN_INACTIVE_TIME);

            users.forEach((userId, userState) -> {
                userState.executions.forEach((executionId, executionState) -> {
                    executionState.runningServants.forEach((servantId, servantState) -> {
                        if (servantState.lastActivityTime.isBefore(maxLastActivityTime)) {
                            LOG.error("Deallocate outdated servant {}", servantId);
                            deallocate(servantId);
                        } else if (servantState.lastActivityTime.isBefore(warnLastActivityTime)) {
                            LOG.warn("There is no any activity from servant {} for {}",
                                servantId, Duration.between(servantState.lastActivityTime, now));
                        }
                    });
                });
            });

            runningServantsTimer.schedule(this, Duration.ofMinutes(2).toMillis());
        }
    }
}
