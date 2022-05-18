package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.TaskCredentials;

import static org.junit.Assert.*;

public class DbAuthServiceTest {

    private ApplicationContext ctx;
    private AuthenticateService authenticateService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        authenticateService = ctx.getBean(DbAuthService.class);
    }

    @Test
    public void authenticate() {
        authenticateService.authenticate(new TaskCredentials(""));
    }

    @After
    public void tearDown() {
        ctx.stop();
    }
}