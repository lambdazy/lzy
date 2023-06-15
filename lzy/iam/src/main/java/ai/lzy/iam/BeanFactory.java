package ai.lzy.iam;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {
    public static final String TEST_ENV_NAME = "local-test";

    @Singleton
    @Named("IamIdGenerator")
    public IdGenerator idGenerator() {
        return new RandomIdGenerator();
    }
}
