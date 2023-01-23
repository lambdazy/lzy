package ai.lzy.scheduler.test;

import ai.lzy.scheduler.JobService;
import ai.lzy.scheduler.db.JobsOperationDao;
import ai.lzy.scheduler.providers.WorkflowJobProvider;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.function.Function;

@Singleton
public class A extends WorkflowJobProvider<Provider.Data> {

    public static Function<Provider.Data, Provider.Data> onExecute = (a) -> a;
    public static Function<Provider.Data, Provider.Data> onClear = (a) -> a;

    @Inject
    public A(JobService jobService, Provider.DataSerializer serializer,
             JobsOperationDao dao, ApplicationContext context)
    {
        super(jobService, serializer, dao, null, B.class, context);
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
