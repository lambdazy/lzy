package ai.lzy.test;

import ai.lzy.service.workflow.WorkflowService;
import ai.lzy.test.impl.v2.PythonContext;
import ai.lzy.worker.env.CondaEnvironment;

public class RunAllServices {
    public static void main(String[] args) {
        WorkflowService.PEEK_RANDOM_PORTAL_PORTS = true;  // To recreate portals for all wfs
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration
        
        ApplicationContextRule ctx = new ApplicationContextRule();
        final PythonContext pythonContext = ctx.getCtx().getBean(PythonContext.class);
        Runtime.getRuntime().addShutdownHook(new Thread(pythonContext::close));
    }
}
