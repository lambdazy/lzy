package ai.lzy.allocator.test;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.retry.annotation.Retryable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RetryTest {
    private RetryableClass instance;

    @Before
    public void before() {
        final ApplicationContext context = ApplicationContext.run();
        instance = context.getBean(RetryableClass.class);
    }

    @Bean
    public static class RetryableClass {
        private int counter = 0;

        @Retryable(attempts = "4", includes = IllegalArgumentException.class)
        public int retryableMethod() throws IllegalArgumentException {
            if (counter < 3) {
                counter += 1;
                throw new IllegalArgumentException();
            }
            return counter;
        }
    }

    @Test
    public void retryTest() {
        Assert.assertEquals(3, instance.retryableMethod());
    }
}
