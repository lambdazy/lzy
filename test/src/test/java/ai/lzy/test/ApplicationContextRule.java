package ai.lzy.test;

import io.micronaut.context.ApplicationContext;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ApplicationContextRule extends TestWatcher {
    private ApplicationContext ctx;

    @Override
    protected void finished(Description description) {
        ctx.close();
    }

    public ApplicationContext getCtx() {
        if (ctx == null) {
            ctx = ApplicationContext.run();
        }
        return ctx;
    }
}
