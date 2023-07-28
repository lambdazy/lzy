package ai.lzy.test;

import ai.lzy.test.context.PythonContextTests;
import org.junit.Test;

public class UserImagePyTest extends PythonContextTests {
    @Test
    public void testUserImage() {
        /* This scenario checks for:
                1. Execution is running inside container from user image
                2. External modules are available inside container
         */
        evalAndAssertScenarioResult("user_image_cpu");
    }
}
