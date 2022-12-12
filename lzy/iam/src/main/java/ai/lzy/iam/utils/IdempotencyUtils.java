package ai.lzy.iam.utils;

import com.google.protobuf.Message;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public enum IdempotencyUtils {
    ;

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
