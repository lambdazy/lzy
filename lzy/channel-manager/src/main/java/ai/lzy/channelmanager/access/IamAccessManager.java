package ai.lzy.channelmanager.access;

import ai.lzy.channelmanager.operation.ChannelOperation;
import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Singleton
public class IamAccessManager {

    private final SubjectServiceClient iamSubjectClient;
    private final AccessClient iamAccessClient;

    @Inject
    public IamAccessManager(@Named("ChannelManagerIamSubjectClient") SubjectServiceClient subjectClient,
                            @Named("ChannelManagerIamAccessClient") AccessClient iamAccessClient)
    {
        this.iamSubjectClient = subjectClient;
        this.iamAccessClient = iamAccessClient;
    }

    public boolean checkAccess(String subjId, String userId, String workflowName, ChannelOperation.Type opType) {
        // TODO: retries
        final var permission = switch (opType) {
            case BIND, UNBIND -> AuthPermission.WORKFLOW_RUN;
            case DESTROY -> AuthPermission.WORKFLOW_STOP;
        };

        return checkAccess(subjId, userId, workflowName, permission);
    }

    public boolean checkAccess(String subjId, String userId, String workflowName, AuthPermission permission) {

        final var subj = iamSubjectClient.getSubject(subjId);
        final var resource = new Workflow(userId + "/" + workflowName);

        try {
            return iamAccessClient.hasResourcePermission(subj, resource, permission);
        } catch (AuthNotFoundException | AuthPermissionDeniedException e) {
            return false;
        }
    }
}
