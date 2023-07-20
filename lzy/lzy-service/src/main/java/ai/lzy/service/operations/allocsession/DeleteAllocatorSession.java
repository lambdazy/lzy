package ai.lzy.service.operations.allocsession;

import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.DeleteAllocatorSessionState;
import ai.lzy.service.operations.ExecutionOperationRunner;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.grpc.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class DeleteAllocatorSession extends ExecutionOperationRunner {
    private final DeleteAllocatorSessionState state;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final List<Supplier<StepResult>> steps;

    protected DeleteAllocatorSession(DeleteAllocatorSessionBuilder builder) {
        super(builder);
        this.state = builder.state;
        this.allocOpClient = builder.allocOpClient;
        this.steps = List.of(deleteAllocSession(), waitDeleteAllocSession(), this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
    }

    private StepResult complete() {
        var response = Any.pack(Empty.getDefaultInstance());
        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    deleteAllocatorSessionOpsDao().delete(id(), tx);
                    completeOperation(null, response, tx);
                    tx.commit();
                }
            });
        } catch (Exception e) {
            var sqlError = e instanceof SQLException;

            log().error("{} Cannot complete successful DeleteAllocatorSession operation: {}.{}",
                logPrefix(), e.getMessage(), (sqlError ? " Reschedule..." : ""));

            return sqlError ? StepResult.RESTART : StepResult.FINISH;
        }

        return StepResult.FINISH;
    }

    @Override
    protected boolean fail(Status status) {
        log().error("{} Fail DeleteAllocatorSession operation: {}", logPrefix(), status.getDescription());

        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    deleteAllocatorSessionOpsDao().delete(id(), tx);
                    failOperation(status, tx);
                    tx.commit();
                }
            });
            return true;
        } catch (OperationCompletedException ex) {
            log().error("{} Cannot fail operation: already completed", logPrefix());
            return true;
        } catch (NotFoundException ex) {
            log().error("{} Cannot fail operation: not found", logPrefix());
            return true;
        } catch (Exception ex) {
            log().error("{} Cannot fail operation: {}. Retry later...", logPrefix(), ex.getMessage());
            return false;
        }
    }

    private Supplier<StepResult> deleteAllocSession() {
        return new DeleteAllocatorSessionStep(stepCtx(), state, allocClient());
    }

    private Supplier<StepResult> waitDeleteAllocSession() {
        return new WaitDeleteAllocatorSessionStep(stepCtx(), state, allocOpClient);
    }

    public static DeleteAllocatorSessionBuilder builder() {
        return new DeleteAllocatorSessionBuilder();
    }

    public static final class DeleteAllocatorSessionBuilder
        extends ExecutionOperationRunnerBuilder<DeleteAllocatorSessionBuilder>
    {
        private DeleteAllocatorSessionState state;
        private LongRunningServiceBlockingStub allocOpClient;

        @Override
        public DeleteAllocatorSession build() {
            return new DeleteAllocatorSession(this);
        }

        public DeleteAllocatorSessionBuilder setState(DeleteAllocatorSessionState state) {
            this.state = state;
            return this;
        }

        public DeleteAllocatorSessionBuilder setAllocOpClient(LongRunningServiceBlockingStub allocOpClient) {
            this.allocOpClient = allocOpClient;
            return this;
        }
    }
}
