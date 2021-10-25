package ru.yandex.cloud.ml.platform.lzy.server.hibernate;


import org.hibernate.Session;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;

import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class UserDAO {
    private final Storage storage;

    UserDAO(Storage storage){
        this.storage = storage;
    }

    public boolean isUserTokenSigned(String userId, String token, String tokenSign)
    {
        try (Session session = storage.getSessionFactory().openSession()) {
            final UserModel user;
            user = session.find(UserModel.class, userId);
            if (user == null) {
                return false;
            }

            Security.addProvider(
                    new org.bouncycastle.jce.provider.BouncyCastleProvider()
            );

            final byte[] publicKeyPEM = Base64.getDecoder().decode(
                    user.getPublicToken().replaceAll("-----[^-]*-----\\n", "")
                            .replaceAll("\\R", "")
            );
            try {
                final PublicKey rsaKey = KeyFactory.getInstance("RSA")
                        .generatePublic(new X509EncodedKeySpec(publicKeyPEM));

                final Signature sign = Signature.getInstance("SHA1withRSA");
                sign.initVerify(rsaKey);
                sign.update(token.getBytes());

                return sign.verify(Base64.getDecoder().decode(tokenSign));
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
