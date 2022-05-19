package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthBadRequestException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.credentials.UserCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

import static org.junit.Assert.*;

public class DbSubjectServiceTest {
    public static final Logger LOG = LogManager.getLogger(DbSubjectServiceTest.class);

    private ApplicationContext ctx;
    private SubjectService subjectService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        subjectService = ctx.getBean(DbSubjectService.class);
    }

    @After
    public void tearDown() {
        ctx.stop();
    }

    @Test
    public void createAndDeleteTest() {
        create("1");
        create("2");

        Subject user1 = subjectService.subject("1");
        assertEquals("1", user1.id());
        Subject user2 = subjectService.subject("2");
        assertEquals("2", user2.id());

        subjectService.removeSubject(user1);
        try {
            subjectService.subject("1");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        user2 = subjectService.subject("2");
        assertEquals("2", user2.id());

        subjectService.removeSubject(user2);
        try {
            subjectService.subject("2");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
    }

    @Test
    public void createAndRemoveWithCredentials() {
        create("1");
        Subject user = subjectService.subject("1");
        addCredentials(user, "1");
        addCredentials(user, "2");

        UserCredentials credentials1 = subjectService.credentials(user, "1");
        assertEquals("1", credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals("Type", credentials1.type());
        UserCredentials credentials2 = subjectService.credentials(user, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        subjectService.removeCredentials(user, "1");
        try {
            subjectService.credentials(user,"1");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        credentials2 = subjectService.credentials(user, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        subjectService.removeCredentials(user, "2");
        try {
            subjectService.credentials(user,"2");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
        subjectService.removeSubject(user);
    }

    private void create(String id) {
        subjectService.createSubject(id, "provider", "providerID");
    }

    private void addCredentials(Subject subject, String name) {
        subjectService.addCredentials(subject, name, "Value", "Type");
    }
}