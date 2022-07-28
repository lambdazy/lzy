package ai.lzy.scheduler.exp;

import ai.lzy.model.graph.Provisioning;
import com.google.common.net.HostAndPort;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface ServantAllocatorExp {

    record AllocatedServant(
        String servantId,
        HostAndPort apiEndpoint,
        HostAndPort fsApiEndpoint,
        Map<String, String> meta
    ) {}

    AllocatedServant allocate(String userId, String executionId, Provisioning provisioning, Duration timeout)
        throws TimeoutException, ServantAllocationException;

    class ServantAllocationException extends Exception {
        public ServantAllocationException(String message) {
            super(message);
        }

        public ServantAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    interface Ex extends ServantAllocatorExp {
        void register(String servantId, String ott, HostAndPort apiEndpoint, HostAndPort fsApiEndpoint,
                      Map<String, String> meta);

        enum ServantState {
            Start,
            StartTask,
            ExecuteTask,
            FinishTask,
            Idle,
            Finish
        }

        void report(String servantId, ServantState state);
    }
}
