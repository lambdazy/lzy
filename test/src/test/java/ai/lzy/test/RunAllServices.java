package ai.lzy.test;

import ai.lzy.test.impl.v2.PythonContext;

public class RunAllServices {
    public static void main(String[] args) {
        ApplicationContextRule ctx = new ApplicationContextRule();
        final PythonContext pythonContext = ctx.getCtx().getBean(PythonContext.class);
        Runtime.getRuntime().addShutdownHook(new Thread(pythonContext::close));
    }
}
