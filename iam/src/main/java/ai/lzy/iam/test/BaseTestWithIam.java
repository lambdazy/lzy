package ai.lzy.iam.test;

import ai.lzy.iam.LzyIAM;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import org.junit.After;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public class BaseTestWithIam {

    private ApplicationContext iamCtx;
    private LzyIAM iamApp;

    public void before() throws IOException, InterruptedException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var iamProps = new YamlPropertySourceLoader()
            .read("iam", new FileInputStream("../iam/src/main/resources/application-test.yml"));
        iamProps.putAll(overrides);
        iamCtx = ApplicationContext.run(PropertySource.of(iamProps));
        iamApp = new LzyIAM(iamCtx);
        iamApp.start();
    }

    @After
    public void after() {
        iamApp.close();
        iamCtx.close();
    }
}
