package ai.lzy.test;

import ai.lzy.iam.LzyIAM;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import org.junit.After;
import org.junit.Before;

import java.io.FileInputStream;
import java.io.IOException;

public class BaseTestWithIam {

    private ApplicationContext iamCtx;
    private LzyIAM iamApp;

    @Before
    public void before() throws IOException {
        var iamProps = new YamlPropertySourceLoader()
            .read("iam", new FileInputStream("../iam/src/main/resources/application-test.yml"));
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
