package ai.lzy.allocator.services;

import ai.lzy.allocator.alloc.AllocationContext;
import ai.lzy.longrunning.OperationsExecutor;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;

import java.time.Duration;

@Requires(property = "allocator.enable-http-debug", value = "true")
@Controller(value = "/debug/actions", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
public class DebugActionsController {
    private final AllocationContext allocationContext;

    @Inject
    public DebugActionsController(AllocationContext allocationContext) {
        this.allocationContext = allocationContext;
    }

    @Post("/long-action")
    public void longAction(@QueryValue("duration") int durationSec) {
        allocationContext.executor().startNew(() -> {
            System.err.println("[DEBUG] Start long action with duration " + durationSec + " seconds...");
            try {
                Thread.sleep(Duration.ofSeconds(durationSec).toMillis());
                System.err.println("[DEBUG] Long action completed.");
            } catch (InterruptedException e) {
                System.err.println("[DEBUG] Long action was interrupted: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        });
    }

    @Post("/seq-actions")
    public void seqActions(@QueryValue("count") int count) {
        allocationContext.executor().startNew(new SeqAction(count, 10, allocationContext.executor()));
    }

    private static final class SeqAction implements Runnable {
        private int n;
        private final int durationSec;
        private final OperationsExecutor executor;

        private SeqAction(int n, int durationSec, OperationsExecutor executor) {
            this.n = n;
            this.durationSec = durationSec;
            this.executor = executor;
        }

        @Override
        public void run() {
            System.err.println("[DEBUG] Run SeqAction #" + n);
            try {
                Thread.sleep(Duration.ofSeconds((int) (Math.random() * durationSec)).toMillis());
            } catch (InterruptedException e) {
                System.err.println("[DEBUG] SeqAction #" + n + " was interrupted: " + e.getMessage());
                e.printStackTrace(System.err);
                return;
            }

            if (--n > 0) {
                executor.retryAfter(this, Duration.ofMillis(100 + (int) (Math.random() * 1000)));
            }
        }
    }
}
