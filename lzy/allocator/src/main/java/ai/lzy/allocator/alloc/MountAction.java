package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.volume.VolumeManager;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.longrunning.Operation;
import ai.lzy.longrunning.OperationRunnerBase;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public final class MountAction extends OperationRunnerBase {
    private final AllocationContext allocationContext;
    private final VolumeManager volumeManager;

    private State state;

    private record State(
        Volume volume
    ) {}

    public MountAction(String diskId, String vmId, String opId, AllocationContext allocationContext) {
        super(opId, String.format("Disk %s to VM %s", diskId, vmId),
            allocationContext.storage(), allocationContext.operationsDao(), allocationContext.executor());

        this.allocationContext = allocationContext;

        log().info("{} Mount disk...", logPrefix());
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of();
    }

    @Override
    protected boolean isInjectedError(Error e) {
        return e instanceof InjectedFailures.TerminateException;
    }

    @Override
    protected void notifyExpired() {
    }

    @Override
    protected void notifyFinished() {
    }

    @Override
    protected void onExpired(TransactionHandle tx) throws SQLException {
        // not to delete pv, pvc
        // create fictive pod, if deleted now
    }

    @Override
    protected void onCompletedOutside(Operation op, TransactionHandle tx) throws SQLException {
        // create fictive pod, if deleted now
    }



    private StepResult createVolumeWithClaim() {
        if (state.volume() != null) {
            return StepResult.ALREADY_DONE;
        }

        var volumeDesc = new VolumeRequest.ResourceVolumeDescription(

        )

        volumeManager.create()

        String opId;

        return null; // TODO
    }

    private StepResult createVolumeClaim() {
        // InjectedFailure

        return null; // TODO
    }

    private StepResult recreateFictivePod() {
        // InjectedFailure

        return null; // TODO
    }

    private void fail(Status status) throws Exception {
        log().error("{} Fail VM allocation operation: {}", logPrefix(), status.getDescription());
        withRetries(log(), () -> {
            try (var tx = TransactionHandle.create(allocationContext.storage())) {
                failOperation(status, tx);
                prepareDeleteVmAction(status.getDescription(), tx);
                tx.commit();
            }
        });
    }
}
