package ai.lzy.test;

import ai.lzy.test.context.PythonContextTests;
import org.junit.Test;

import java.util.List;

public class ClusterTest extends PythonContextTests {
    @Override
    protected String endpoint() {
        return System.getenv("ENDPOINT");
    }

    @Override
    protected String wbEndpoint() {
        return System.getenv("WB_ENDPOINT");
    }

    @Override
    protected String username() {
        return System.getenv("USERNAME");
    }

    @Override
    protected String keyPath() {
        return System.getenv("KEY_PATH");
    }

    @Test
    public void testTwoExecutionOneWorkflow() {
        evalAndAssertScenarioResult("two_execution_one_wf");
    }

    @Test
    public void testCatboostGraphGpu() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
                3. Execution is running on gpu
         */
        evalAndAssertScenarioResult("catboost_integration_gpu", List.of("catboost"));
    }

    @Test
    public void testUserImageGpu() {
        /* This scenario checks for:
                1. Execution is running inside container from user image
                2. Execution is running on gpu
         */
        evalAndAssertScenarioResult("user_image_gpu", List.of("tensorflow"));
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        evalAndAssertScenarioResult("file_test");
    }
}
