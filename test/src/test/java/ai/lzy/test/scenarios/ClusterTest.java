package ai.lzy.test.scenarios;

import ai.lzy.test.impl.v2.PythonContextBase;
import org.junit.Test;

import java.util.List;

public class ClusterTest {

    private static final PythonContextBase pythonContext = new PythonContextBase(
        System.getenv("ENDPOINT"),
        System.getenv("WB_ENDPOINT"),
        System.getenv("USERNAME"),
        System.getenv("KEY_PATH")
    );

    @Test
    public void testTwoExecutionOneWorkflow() {
        pythonContext.evalAndAssertScenarioResult("two_execution_one_wf");
    }

    @Test
    public void testCatboostGraphGpu() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
                3. Execution is running on gpu
         */
        pythonContext.evalAndAssertScenarioResult("catboost_integration_gpu", List.of("catboost"));
    }

    @Test
    public void testUserImageGpu() {
        /* This scenario checks for:
                1. Execution is running inside container from custom image
                2. Execution is running on gpu
         */
        pythonContext.evalAndAssertScenarioResult("custom_image_gpu");
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        pythonContext.evalAndAssertScenarioResult("file_test");
    }

}
