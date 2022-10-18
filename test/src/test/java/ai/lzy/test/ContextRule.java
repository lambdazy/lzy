package ai.lzy.test;

import io.micronaut.context.ApplicationContext;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ContextRule<CLS> extends TestWatcher {
    private final ApplicationContextRule ctx;
    private final Class<CLS> cls;

    private CLS instance;

    public ContextRule(ApplicationContextRule ctx, Class<CLS> cls) {
        this.ctx = ctx;
        this.cls = cls;
    }

    @Override
    protected void starting(Description description) {
        instance = ctx.getCtx().getBean(cls);
    }

    public CLS context() {
        return instance;
    }
}
