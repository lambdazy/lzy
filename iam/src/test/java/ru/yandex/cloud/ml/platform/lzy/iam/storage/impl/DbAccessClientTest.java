package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthBadRequestException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.*;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Workflow;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.Storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DbAccessClientTest {
    public static final Logger LOG = LogManager.getLogger(DbAccessClientTest.class);

    private ApplicationContext ctx;
    private SubjectService subjectService;
    private Storage storage;
    private AccessClient accessClient;
    private AccessBindingClient accessBindingClient;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(Storage.class);
        subjectService = ctx.getBean(DbSubjectService.class);
        accessClient = ctx.getBean(DbAccessClient.class);
        accessBindingClient = ctx.getBean(DbAccessBindingClient.class);
    }

    @Test
    public void validAccess() throws Exception {
        String userId = "user1";
        AuthResource workflowResource = new Workflow("workflow");
        AuthResource whiteboardResource = new Whiteboard("whiteboard");
        subjectService.createSubject(userId, "", "");
        final Subject user = subjectService.subject(userId);

        accessBindingClient.setAccessBindings(whiteboardResource, List.of(
                new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user)
        ));
        assertTrue(accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_GET));
        assertTrue(accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_UPDATE));
        assertTrue(accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_CREATE));
        assertTrue(accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_DELETE));

        List<AccessBinding> workflowAccessBinding = List.of(
                new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user)
        );
        accessBindingClient.setAccessBindings(workflowResource, workflowAccessBinding);
        assertTrue(accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_RUN));
        assertTrue(accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_STOP));
        assertTrue(accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_DELETE));
        assertTrue(accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_GET));
        assertEquals(workflowAccessBinding, accessBindingClient.listAccessBindings(workflowResource).collect(Collectors.toList()));
    }

    @Test
    public void invalidAccess() throws Exception {
        String userId = "user1";
        AuthResource workflowResource = new Workflow("workflow");
        AuthResource whiteboardResource = new Whiteboard("whiteboard");
        subjectService.createSubject(userId, "", "");
        final Subject user = subjectService.subject(userId);

        accessBindingClient.setAccessBindings(whiteboardResource, List.of(
                new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user)
        ));
        assertTrue(accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_GET));

        accessBindingClient.updateAccessBindings(whiteboardResource, List.of(
                new AccessBindingDelta(
                        AccessBindingDelta.AccessBindingAction.REMOVE,
                        new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user))
        ));
        try {
            accessClient.hasResourcePermission(user, whiteboardResource.resourceId(), AuthPermission.WHITEBOARD_GET);
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception::{}", e.getInternalDetails());
        }

        accessBindingClient.setAccessBindings(workflowResource, List.of(
                new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user)
        ));
        assertTrue(accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_RUN));

        accessBindingClient.updateAccessBindings(workflowResource, List.of(
                new AccessBindingDelta(
                        AccessBindingDelta.AccessBindingAction.REMOVE,
                        new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user))
        ));
        try {
            accessClient.hasResourcePermission(user, workflowResource.resourceId(), AuthPermission.WORKFLOW_RUN);
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception::{}", e.getInternalDetails());
        }
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
}