package ai.lzy.iam;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.exceptions.AuthBadRequestException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class BaseSubjectServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(BaseSubjectServiceApiTest.class);

    @Test
    public void createAndDeleteTest() {
        createAndDeleteScenario(SubjectType.USER);
        createAndDeleteScenario(SubjectType.SERVANT);
    }

    public void createAndDeleteScenario(SubjectType subjectType) {
        createSubject("1", subjectType);
        createSubject("2", subjectType);

        Subject user1 = subject("1");
        assertEquals("1", user1.id());
        Subject user2 = subject("2");
        assertEquals("2", user2.id());

        removeSubject(user1);
        try {
            subject("1");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        user2 = subject("2");
        assertEquals("2", user2.id());

        removeSubject(user2);
        try {
            subject("2");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
    }

    @Test
    public void createAndRemoveWithCredentialsTest() {
        createAndRemoveWithCredentialsScenario(SubjectType.USER);
        createAndRemoveWithCredentialsScenario(SubjectType.SERVANT);
    }

    public void createAndRemoveWithCredentialsScenario(SubjectType subjectType) {
        createSubject("1", subjectType);
        Subject user = subject("1");
        addCredentials(user, "1");
        addCredentials(user, "2");

        SubjectCredentials credentials1 = credentials(user, "1");
        assertEquals("1", credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals("Type", credentials1.type());
        SubjectCredentials credentials2 = credentials(user, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        removeCredentials(user, "1");
        try {
            credentials(user, "1");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        credentials2 = credentials(user, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        removeCredentials(user, "2");
        try {
            credentials(user, "2");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
        removeSubject(user);
    }

    @Test
    public void restrictEqualSubjectIdsTest() {
        restrictEqualSubjectIdsScenario(SubjectType.SERVANT, SubjectType.SERVANT);
        restrictEqualSubjectIdsScenario(SubjectType.SERVANT, SubjectType.USER);
        restrictEqualSubjectIdsScenario(SubjectType.USER, SubjectType.SERVANT);
        restrictEqualSubjectIdsScenario(SubjectType.USER, SubjectType.USER);
    }

    public void restrictEqualSubjectIdsScenario(SubjectType subjectType1, SubjectType subjectType2) {
        String subjectId = "1";
        createSubject(subjectId, subjectType1);
        Subject user1 = subject(subjectId);

        try {
            createSubject(subjectId, subjectType2);
            fail();
        } catch (AuthInternalException e) {
            LOG.info("Valid exception {}", e.getMessage());
        }

        Subject user2 = subject(subjectId);
        assertEquals(user1, user2);

        removeSubject(user1);
    }

    protected abstract Subject subject(String id);

    protected abstract void createSubject(String id, SubjectType subjectType);

    protected abstract void removeSubject(Subject subject);

    protected abstract SubjectCredentials credentials(Subject subject, String id);

    protected abstract void addCredentials(Subject subject, String name);

    protected abstract void removeCredentials(Subject subject, String name);
}
