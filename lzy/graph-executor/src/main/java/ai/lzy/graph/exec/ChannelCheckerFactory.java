package ai.lzy.graph.exec;

import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.exec.impl.DirectChannelChecker;
import ai.lzy.graph.model.ChannelDescription;
import ai.lzy.graph.model.TaskExecution;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
public class ChannelCheckerFactory {

    private final SchedulerApi api;

    @Inject
    public ChannelCheckerFactory(SchedulerApi api) {
        this.api = api;
    }

    public ChannelChecker checker(
        Map<String, TaskExecution> taskDescIdToTaskExec,
        String workflowId,
        ChannelDescription channel
    )
    {
        return switch (channel.type()) {
            case DIRECT -> new DirectChannelChecker(api, taskDescIdToTaskExec, workflowId);
        };
    }
}
