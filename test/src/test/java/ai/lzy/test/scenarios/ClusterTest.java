package ai.lzy.test.scenarios;

import ai.lzy.test.impl.v2.PythonContextBase;
import ai.lzy.worker.env.CondaEnvironment;
import org.junit.AfterClass;
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
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */
        pythonContext.evalAndAssertScenarioResult("catboost_integration_cpu", List.of("catboost"));
    }

    @Test
    public void testExecFail() {
        pythonContext.evalAndAssertScenarioResult("exec_fail");
    }

    @Test
    public void testEnvFail() {
        pythonContext.evalAndAssertScenarioResult("env_fail");
    }

    @Test
    public void testCustomCondaAndSerializer() {

        pythonContext.evalAndAssertScenarioResult("custom_conda_and_serializer");

    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package
         */

        pythonContext.evalAndAssertScenarioResult("import");
    }

    @Test
    public void testNoneResult() {
        /* This scenario checks for:
                1. Calling @op with None as result
         */

        //Arrange
        pythonContext.evalAndAssertScenarioResult("none_result");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */
        pythonContext.evalAndAssertScenarioResult("whiteboards");
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        pythonContext.evalAndAssertScenarioResult("file_test");
    }

}
