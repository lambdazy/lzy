package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ai.lzy.iam.authorization.exceptions.AuthBadRequestException;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Subject;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DbSubjectServiceTest {
    public static final Logger LOG = LogManager.getLogger(DbSubjectServiceTest.class);

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
    }

    @After
    public void tearDown() {
        try (PreparedStatement st = storage.connect().prepareStatement("DROP ALL OBJECTS DELETE FILES;")) {
            st.executeUpdate();
        } catch (SQLException e) {
            LOG.error(e);
        }
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

        SubjectCredentials credentials1 = subjectService.credentials(user, "1");
        assertEquals("1", credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals("Type", credentials1.type());
        SubjectCredentials credentials2 = subjectService.credentials(user, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        subjectService.removeCredentials(user, "1");
        try {
            subjectService.credentials(user, "1");
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
            subjectService.credentials(user, "2");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
        subjectService.removeSubject(user);
    }

    private void create(String id) {
        subjectService.createSubject(id, "provider", "providerID", SubjectType.USER);
    }

    private void addCredentials(Subject subject, String name) {
        subjectService.addCredentials(subject, name, "Value", "Type");
    }
}
