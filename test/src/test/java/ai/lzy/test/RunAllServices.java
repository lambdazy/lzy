package ai.lzy.test;

import ai.lzy.test.impl.v2.PythonContext;
import ai.lzy.worker.env.CondaEnvironment;

public class RunAllServices {
    public static void main(String[] args) {
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration
        
        ApplicationContextRule ctx = new ApplicationContextRule();
        final PythonContext pythonContext = ctx.getCtx().getBean(PythonContext.class);
        Runtime.getRuntime().addShutdownHook(new Thread(pythonContext::close));
    }
}
