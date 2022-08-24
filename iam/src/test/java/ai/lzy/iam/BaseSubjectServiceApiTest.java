package ai.lzy.iam;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.exceptions.AuthBadRequestException;
import ai.lzy.util.auth.exceptions.AuthInternalException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(JUnitParamsRunner.class)
public abstract class BaseSubjectServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(BaseSubjectServiceApiTest.class);

    @Test
    public void createAndDeleteUserTest() {
        createAndDeleteScenario(SubjectType.USER);
    }

    @Test
    public void createAndDeleteServantTest() {
        createAndDeleteScenario(SubjectType.SERVANT);
    }

    public void createAndDeleteScenario(SubjectType subjectType) {
        createSubject("1", subjectType);
        createSubject("2", subjectType);

        Subject subject1 = subject("1");
        assertEquals("1", subject1.id());
        assertEquals(subjectType, subject1.type());
        Subject subject2 = subject("2");
        assertEquals("2", subject2.id());
        assertEquals(subjectType, subject2.type());

        removeSubject(subject1);
        try {
            subject("1");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        subject2 = subject("2");
        assertEquals("2", subject2.id());
        assertEquals(subjectType, subject2.type());

        removeSubject(subject2);
        try {
            subject("2");
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
    }

    @Test
    public void createAndRemoveUserWithCredentialsTest() {
        createAndRemoveWithCredentialsScenario(SubjectType.USER);
    }

    @Test
    public void createAndRemoveServantWithCredentialsTest() {
        createAndRemoveWithCredentialsScenario(SubjectType.SERVANT);
    }

    public void createAndRemoveWithCredentialsScenario(SubjectType subjectType) {
        createSubject("1", subjectType);
        Subject subject = subject("1");
        addCredentials(subject, "1");
        addCredentials(subject, "2");

        SubjectCredentials credentials1 = credentials(subject, "1");
        assertEquals("1", credentials1.name());
        assertEquals("Value", credentials1.value());
        assertEquals("Type", credentials1.type());
        SubjectCredentials credentials2 = credentials(subject, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        removeCredentials(subject, "1");
        try {
            credentials(subject, "1");
            fail();
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }

        credentials2 = credentials(subject, "2");
        assertEquals("2", credentials2.name());
        assertEquals("Value", credentials2.value());
        assertEquals("Type", credentials2.type());

        removeCredentials(subject, "2");
        try {
            credentials(subject, "2");
            fail();
        } catch (NoSuchElementException e) {
            LOG.info("Valid exception {}", e.getMessage());
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception {}", e.getInternalDetails());
        }
        removeSubject(subject);
    }

    public static Collection<Object[]> restrictEqualSubjectIdsParams() {
        return Arrays.asList(
                new Object[][]{
                        {SubjectType.USER, SubjectType.USER},
                        {SubjectType.USER, SubjectType.SERVANT},
                        {SubjectType.SERVANT, SubjectType.USER},
                        {SubjectType.SERVANT, SubjectType.SERVANT}
                }
        );
    }

    @Test
    @Parameters(method = "restrictEqualSubjectIdsParams")
    public void restrictEqualSubjectIdsTest(SubjectType subjectType1, SubjectType subjectType2) {
        String subjectId = "1";
        createSubject(subjectId, subjectType1);
        Subject subject1 = subject(subjectId);

        try {
            createSubject(subjectId, subjectType2);
            fail();
        } catch (AuthInternalException e) {
            LOG.info("Valid exception {}", e.getMessage());
        }

        Subject subject2 = subject(subjectId);
        assertEquals(subject1, subject2);

        removeSubject(subject1);
    }

    protected abstract Subject subject(String id);

    protected abstract void createSubject(String id, SubjectType subjectType);

    protected abstract void removeSubject(Subject subject);

    protected abstract SubjectCredentials credentials(Subject subject, String name) throws NoSuchElementException;

    protected abstract void addCredentials(Subject subject, String name);

    protected abstract void removeCredentials(Subject subject, String name);
}
