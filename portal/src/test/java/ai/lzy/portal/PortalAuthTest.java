package ai.lzy.portal;

import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.portal.LzyPortalApi;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class PortalAuthTest extends PortalTestBase {

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testUnauthenticated() {
        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedPortalClient.openSlots(
                    LzyPortalApi.OpenSlotsRequest.newBuilder().build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedPortalClient.status(
                    LzyPortalApi.PortalStatusRequest.newBuilder().build()
                )));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode()));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPermissionDenied() {
        var client = unauthorizedPortalClient.withInterceptors(ClientHeaderInterceptor.header(
            GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user", "GITHUB")::token));

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.openSlots(
                    LzyPortalApi.OpenSlotsRequest.newBuilder().build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.status(
                    LzyPortalApi.PortalStatusRequest.newBuilder().build()
                )));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode()));
    }

}
