package ai.lzy.test;

import io.micronaut.context.ApplicationContext;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ApplicationContextRule extends TestWatcher {
    private ApplicationContext ctx;

    @Override
    protected void starting(Description description) {
        ctx = ApplicationContext.run();
    }

    @Override
    protected void finished(Description description) {
        ctx.close();
    }

    public ApplicationContext getCtx() {
        return ctx;
    }
}
