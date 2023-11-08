package ai.lzy.test;

import ai.lzy.env.aux.CondaEnvironment;
import ai.lzy.slots.transfers.SlotInputTransfer;
import ai.lzy.test.context.PythonContextTests;
import org.junit.Test;

import java.util.List;

public class PyApiTest extends PythonContextTests {
    static {
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration

        SlotInputTransfer.setMaxRetryAttempts(2);
    }

    @Test
    public void testTwoExecutionOneWorkflow() {
        evalAndAssertScenarioResult("two_execution_one_wf");
    }

    @Test
    public void testRepeatedExecsUseCache() {
        evalAndAssertScenarioResult("repeated_execs_use_cache");
    }

    @Test
    public void testRepeatedOpsUseCache() {
        evalAndAssertScenarioResult("repeated_ops_use_cache");
    }

    @Test
    public void testFullyCachedGraph() {
        evalAndAssertScenarioResult("fully_cached_graph");
    }

    @Test
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */
        evalAndAssertScenarioResult("catboost_integration_cpu", List.of("catboost"));
    }

    @Test
    public void testExecFail() {
        evalAndAssertScenarioResult("exec_fail");
    }

    @Test
    public void testEnvFail() {
        CondaEnvironment.reconfigureConda(true);
        evalAndAssertScenarioResult("env_fail");
        CondaEnvironment.reconfigureConda(false);
    }

    @Test
    public void testCustomSerializer() {
        CondaEnvironment.reconfigureConda(true);
        try {
            evalAndAssertScenarioResult("custom_serializer");
        } finally {
            CondaEnvironment.reconfigureConda(false);
        }
    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package
         */
        CondaEnvironment.reconfigureConda(true);
        evalAndAssertScenarioResult("import");
        CondaEnvironment.reconfigureConda(false);
    }

    @Test
    public void testNestedWorkflows() {
        /* This scenario checks for:
                1. Workflow that is run from another workflow
         */
        CondaEnvironment.reconfigureConda(true);
        evalAndAssertScenarioResult("nested_workflows");
        CondaEnvironment.reconfigureConda(false);
    }

    @Test
    public void testComplexGraph() {
        /* This scenario checks for:
                1. Calling @op with None as result
                2. Iterative graph
         */

        //Arrange
        evalAndAssertScenarioResult("complex_graph");
    }

    @Test
    public void testLargeInputOutput() {
        /* This scenario checks for:
                1. Checking large input & output
         */

        //Arrange
        evalAndAssertScenarioResult("large_input_output");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */
        evalAndAssertScenarioResult("whiteboards");
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        evalAndAssertScenarioResult("file_test");
    }

    @Test
    public void testExceptionSerialize() {
        evalAndAssertScenarioResult("exception_serialize");
    }

    @Test
    public void testCachedException() {
        evalAndAssertScenarioResult("cached_exception");
    }

    @Test
    public void testSubprocessWithStartup() {
        evalAndAssertScenarioResult("subprocess_with_startup");
    }
}
