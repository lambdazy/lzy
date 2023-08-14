package ai.lzy.allocator.alloc;

import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.model.db.TransactionHandle;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface VmAllocator {

    class Result {
        public enum Code {
            SUCCESS,
            RETRY_LATER,
            FAILED
        }

        private final Code code;
        private final String reason;

        public static final Result SUCCESS = new Result(Code.SUCCESS, "");
        public static final Result RETRY_LATER = new Result(Code.RETRY_LATER, "");
        public static final Result FAILED = new Result(Code.FAILED, "");

        private Result(Code code, @Nullable String reason) {
            this.code = code;
            this.reason = reason != null ? reason : "";
        }

        public Code code() {
            return code;
        }

        public String message() {
            return reason;
        }

        public Result withReason(@Nullable String reason) {
            return new Result(code, reason);
        }

        @Override
        public String toString() {
            return "Result{code=%s, reason='%s'}".formatted(code, reason);
        }

        public static Result fromGrpcStatus(Status status) {
            return switch (status.getCode()) {
                case OK, ALREADY_EXISTS
                    -> VmAllocator.Result.SUCCESS;
                case CANCELLED, UNKNOWN, INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, FAILED_PRECONDITION,
                    OUT_OF_RANGE, UNIMPLEMENTED, INTERNAL, DATA_LOSS, UNAUTHENTICATED
                    -> VmAllocator.Result.FAILED.withReason(status.getDescription());
                case DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, UNAVAILABLE
                    -> VmAllocator.Result.RETRY_LATER.withReason(status.getDescription());
            };
        }
    }

    /**
     * Start vm allocation.
     *
     * @throws InvalidConfigurationException on invalid spec
     */
    Result allocate(Vm.Ref vmRef) throws InvalidConfigurationException;

    Result getVmAllocationStatus(Vm vm) throws InvalidConfigurationException;

    Result unmountFromVm(Vm vm, String mountPath) throws InvalidConfigurationException;

    Result bindMountInVm(Vm vm, String fromPath, String toPath, @Nullable String chown)
        throws InvalidConfigurationException;

    /**
     * Idempotent operation to destroy vm
     * If vm is not allocated, does nothing
     *
     * @param vm vm to deallocate
     */
    Result deallocate(Vm vm);

    Vm updateAllocatedVm(Vm vm, @Nullable TransactionHandle tx) throws SQLException;
}
