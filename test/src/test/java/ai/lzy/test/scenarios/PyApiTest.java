package ai.lzy.test.scenarios;

import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.PythonContext;
import ai.lzy.worker.env.CondaEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

public class PyApiTest {
    @ClassRule
    public static final ApplicationContextRule ctx = new ApplicationContextRule();

    @ClassRule
    public static final ContextRule<PythonContext> pythonContext = new ContextRule<>(ctx, PythonContext.class);

    static {
        CondaEnvironment.reconfigureConda(false);  // To optimize conda configuration
    }

    @Test
    public void testTwoExecutionOneWorkflow() {
        pythonContext.context().evalAndAssertScenarioResult("two_execution_one_wf");
    }

    @Test
    public void testRepeatedExecsUseCache() {
        pythonContext.context().evalAndAssertScenarioResult("repeated_execs_use_cache");
    }

    @Test
    public void testRepeatedOpsUseCache() {
        pythonContext.context().evalAndAssertScenarioResult("repeated_ops_use_cache");
    }

    @Test
    public void testFullyCachedGraph() {
        pythonContext.context().evalAndAssertScenarioResult("fully_cached_graph");
    }

    @Test
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */
        pythonContext.context().evalAndAssertScenarioResult("catboost_integration_cpu", List.of("catboost"));
    }

    @Test
    public void testExecFail() {
        pythonContext.context().evalAndAssertScenarioResult("exec_fail");
    }

    @Test
    public void testEnvFail() {
        CondaEnvironment.reconfigureConda(true);
        pythonContext.context().evalAndAssertScenarioResult("env_fail");
        CondaEnvironment.reconfigureConda(false);
    }

    @Test
    public void testCustomCondaAndSerializer() {
        CondaEnvironment.reconfigureConda(true);
        try {
            pythonContext.context().evalAndAssertScenarioResult("custom_conda_and_serializer");
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
        pythonContext.context().evalAndAssertScenarioResult("import");
        CondaEnvironment.reconfigureConda(false);
    }

    @Test
    public void testNestedWorkflows() {
        /* This scenario checks for:
                1. Workflow that is run from another workflow
         */
        CondaEnvironment.reconfigureConda(true);
        pythonContext.context().evalAndAssertScenarioResult("nested_workflows");
        CondaEnvironment.reconfigureConda(false);
    }

    @Test
    public void testComplexGraph() {
        /* This scenario checks for:
                1. Calling @op with None as result
                2. Iterative graph
         */

        //Arrange
        pythonContext.context().evalAndAssertScenarioResult("complex_graph");
    }

    @Test
    public void testLargeInputOutput() {
        /* This scenario checks for:
                1. Checking large input & output
         */

        //Arrange
        pythonContext.context().evalAndAssertScenarioResult("large_input_output");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */
        pythonContext.context().evalAndAssertScenarioResult("whiteboards");
    }

    @Test
    public void testFile() {
        /* This scenario checks for:
                1. File as an argument/return value/whiteboard field
         */
        pythonContext.context().evalAndAssertScenarioResult("file_test");
    }

    @Test
    public void testExceptionSerialize() {
        pythonContext.context().evalAndAssertScenarioResult("exception_serialize");
    }

    @Test
    public void testCachedException() {
        pythonContext.context().evalAndAssertScenarioResult("cached_exception");
    }

    @Test
    public void testSubprocessWithStartup() {
        pythonContext.context().evalAndAssertScenarioResult("subprocess_with_startup");
    }
}
