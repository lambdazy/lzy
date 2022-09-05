package ai.lzy.iam.storage.impl;

import ai.lzy.iam.resources.*;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.db.IamDataSource;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.exceptions.AuthBadRequestException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DbAccessClientTest {
    public static final Logger LOG = LogManager.getLogger(DbAccessClientTest.class);

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext ctx;
    private DbSubjectService subjectService;
    private IamDataSource storage;
    private DbAccessClient accessClient;
    private DbAccessBindingClient accessBindingClient;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("iam", db.getConnectionInfo()));

        storage = ctx.getBean(IamDataSource.class);
        subjectService = ctx.getBean(DbSubjectService.class);
        accessClient = ctx.getBean(DbAccessClient.class);
        accessBindingClient = ctx.getBean(DbAccessBindingClient.class);
    }


    @After
    public void tearDown() {
        DatabaseTestUtils.cleanup(storage);
        ctx.stop();
    }

    @Test
    public void validAccessUser() {
        validAccess(SubjectType.USER);
    }

    @Test
    public void validAccessServant() {
        validAccess(SubjectType.SERVANT);
    }

    public void validAccess(SubjectType subjectType) {
        String userId = "user1";
        subjectService.createSubject(userId, "", "", subjectType);
        final Subject user = subjectService.subject(userId);

        AuthResource whiteboardResource = new Whiteboard("whiteboard");
        accessBindingClient.setAccessBindings(whiteboardResource, List.of(
                new AccessBinding(Role.LZY_WHITEBOARD_OWNER, user)
        ));
        assertTrue(accessClient.hasResourcePermission(
                user,
                whiteboardResource.resourceId(),
                AuthPermission.WHITEBOARD_GET)
        );
        assertTrue(accessClient.hasResourcePermission(
                user,
                whiteboardResource.resourceId(),
                AuthPermission.WHITEBOARD_UPDATE)
        );
        assertTrue(accessClient.hasResourcePermission(
                user,
                whiteboardResource.resourceId(),
                AuthPermission.WHITEBOARD_CREATE)
        );
        assertTrue(accessClient.hasResourcePermission(
                user,
                whiteboardResource.resourceId(),
                AuthPermission.WHITEBOARD_DELETE)
        );

        AuthResource workflowResource = new Workflow("workflow");
        List<AccessBinding> workflowAccessBinding = List.of(
                new AccessBinding(Role.LZY_WORKFLOW_OWNER, user)
        );
        accessBindingClient.setAccessBindings(workflowResource, workflowAccessBinding);
        assertTrue(accessClient.hasResourcePermission(
                user,
                workflowResource.resourceId(),
                AuthPermission.WORKFLOW_RUN)
        );
        assertTrue(accessClient.hasResourcePermission(
                user,
                workflowResource.resourceId(),
                AuthPermission.WORKFLOW_STOP)
        );
        assertTrue(accessClient.hasResourcePermission(
                user,
                workflowResource.resourceId(),
                AuthPermission.WORKFLOW_DELETE)
        );
        assertTrue(accessClient.hasResourcePermission(
                user,
                workflowResource.resourceId(),
                AuthPermission.WORKFLOW_GET)
        );
        assertEquals(
                workflowAccessBinding,
                accessBindingClient.listAccessBindings(workflowResource).collect(Collectors.toList())
        );
    }

    @Test
    public void invalidAccessUser() {
        invalidAccess(SubjectType.USER);
    }

    @Test
    public void invalidAccessServant() {
        invalidAccess(SubjectType.SERVANT);
    }

    public void invalidAccess(SubjectType subjectType) {
        String userId = "user1";
        subjectService.createSubject(userId, "", "", subjectType);
        final Subject user = subjectService.subject(userId);

        AuthResource whiteboardResource = new Whiteboard("whiteboard");
        accessBindingClient.setAccessBindings(whiteboardResource, List.of(
                new AccessBinding(Role.LZY_WHITEBOARD_OWNER, user)
        ));
        assertTrue(accessClient.hasResourcePermission(
                user,
                whiteboardResource.resourceId(),
                AuthPermission.WHITEBOARD_GET)
        );

        accessBindingClient.updateAccessBindings(whiteboardResource, List.of(
                new AccessBindingDelta(
                        AccessBindingDelta.AccessBindingAction.REMOVE,
                        new AccessBinding(Role.LZY_WHITEBOARD_OWNER, user))
        ));
        try {
            accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_GET);
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception::{}", e.getInternalDetails());
        }

        AuthResource workflowResource = new Workflow("workflow");
        accessBindingClient.setAccessBindings(workflowResource, List.of(
                new AccessBinding(Role.LZY_WORKFLOW_OWNER, user)
        ));
        assertTrue(accessClient.hasResourcePermission(
                user,
                workflowResource.resourceId(),
                AuthPermission.WORKFLOW_RUN)
        );

        accessBindingClient.updateAccessBindings(workflowResource, List.of(
                        new AccessBindingDelta(
                                AccessBindingDelta.AccessBindingAction.REMOVE,
                                new AccessBinding(Role.LZY_WORKFLOW_OWNER, user))
                )
        );
        try {
            accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_RUN);
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception::{}", e.getInternalDetails());
        }
    }
}
