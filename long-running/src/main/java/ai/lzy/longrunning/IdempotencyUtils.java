package ai.lzy.longrunning;

import ai.lzy.util.grpc.GrpcHeaders;
import com.google.protobuf.Message;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import javax.annotation.Nullable;

public final class IdempotencyUtils {

    @Nullable
    public static Operation.IdempotencyKey getIdempotencyKey(Message request) {
        var idempotencyToken = GrpcHeaders.getIdempotencyKey();
        if (idempotencyToken != null) {
            return new Operation.IdempotencyKey(idempotencyToken, md5(request));
        }
        return null;
    }

    public static String md5(Message message) {
        try {
            var md5 = MessageDigest.getInstance("MD5");
            var bytes = md5.digest(message.toByteArray());
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
