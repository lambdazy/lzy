package ai.lzy.allocator.test;

import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.v1.AllocatorAdminGrpc;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;

import static ai.lzy.util.auth.credentials.CredentialsUtils.readPrivateKey;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static org.junit.Assert.assertThrows;

public class AllocatorAdminServiceTest extends AllocatorApiTestBase {

    private AllocatorAdminGrpc.AllocatorAdminBlockingStub stub;

    @Before
    public void before() throws IOException {
        super.setUp();

        stub = AllocatorAdminGrpc.newBlockingStub(channel);
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Test
    public void noAccess() {
        // no auth
        var e = assertThrows(StatusRuntimeException.class, () ->
            stub.getActiveImages(Empty.getDefaultInstance()));
        Assert.assertEquals(Status.Code.UNAUTHENTICATED, e.getStatus().getCode());

        // LzyInternal auth
        e = assertThrows(StatusRuntimeException.class, () ->
            newBlockingClient(stub, "xxx", () -> internalUserCreds.get().token())
                .getActiveImages(Empty.getDefaultInstance()));
        Assert.assertEquals(Status.Code.PERMISSION_DENIED, e.getStatus().getCode());
    }

    @Test
    public void adminAccess() throws Exception {
        var adminKeys = RsaUtils.generateRsaKeys();
        var admin = createAdminSubject("admin", adminKeys.publicKey());

        var jwt = new RenewableJwt(admin.id(), admin.provider().name(), Duration.ofDays(1),
            readPrivateKey(adminKeys.privateKey()));
        var images = newBlockingClient(stub, "xxx", () -> jwt.get().token())
            .getActiveImages(Empty.getDefaultInstance());

        System.out.println(images);
    }
}
