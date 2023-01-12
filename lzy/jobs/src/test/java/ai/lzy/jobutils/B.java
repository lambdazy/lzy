package ai.lzy.jobutils;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.jobsutils.providers.WorkflowJobProvider;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.function.Function;

@Singleton
public class B extends WorkflowJobProvider<Provider.Data> {
    public static Function<Provider.Data, Provider.Data> onExecute = (a) -> a;
    public static Function<Provider.Data, Provider.Data> onClear = (a) -> a;

    @Inject
    public B(JobService jobService, Provider.DataSerializer serializer,
             JobsOperationDao dao, ApplicationContext context)
    {
        super(jobService, serializer, dao, null, A.class, context);
    }

    @Override
    protected Provider.Data exec(Provider.Data state, String operationId) {
        return onExecute.apply(state);
    }

    @Override
    protected Provider.Data clear(Provider.Data state, String operationId) {
        return onClear.apply(state);
    }
}
