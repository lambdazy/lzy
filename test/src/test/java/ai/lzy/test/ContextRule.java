package ai.lzy.test;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class ContextRule<T> extends TestWatcher {
    private final ApplicationContextRule ctx;
    private final Class<T> cls;

    private T instance;

    public ContextRule(ApplicationContextRule ctx, Class<T> cls) {
        this.ctx = ctx;
        this.cls = cls;
    }

    @Override
    protected void starting(Description description) {
        instance = ctx.getCtx().getBean(cls);
    }

    public T context() {
        return instance;
    }
}
