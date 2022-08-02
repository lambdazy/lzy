package ai.lzy.test;

import static ai.lzy.model.utils.JwtCredentials.buildJWT;

import ai.lzy.iam.authorization.credentials.JwtCredentials;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class JwtUtils {

    public static JwtCredentials invalidCredentials(String user) {
        var privateKey = """
            -----BEGIN RSA PRIVATE KEY-----
            MIIEowIBAAKCAQEAn5w8xQDuTg9cc7sP3kcIH+ynzEIBSGc6JFhuOd5r82GL7F+i
            30qdaH8U5yWZR1yv9VD47sVj1zXoz/kZrHiRTczpDG3zA2EvNTNhaCD7MoYc/aqy
            hqQMr9UnS8NVn1JaZfQZJWD9rSIAKlQIFrrSaE9A6K3bdj6JXSNR8A9U9KaAg5zJ
            IgcBuBP0llUSwG0XjOMZyQrTNQmtDkvoqjVR+x5cHmwui+Y0ICJUrzOiXq8ukDHM
            1SCvWu00+c8y2tG2ce1V8eA0EFhk/pMV/q9/JKarU3Xx8wGlrArYePognSdyRPil
            yElr/QoSio+rwAuOwxnuNxB5s0CHLW8qnqhV6wIDAQABAoIBAGKI5q9MWtIQA6hi
            xHIZ8fcbd5/O49HaAHftq+bH3Gb9Qo+jnv4wpyqawcHNYWo/21UcLwHhFDkJS/gQ
            tXvXVwTryrfkrNDaT3WNicXqDonrZ7xmhB5A6qAmfEL2jUZ1Zd9pKZj83r7ira10
            ASZfIYRJ4S2EH2dJRi4cnvoPzQfXRQft4R580Y9oMt2L2dlL7R3n99yMVoNVLRZq
            Nf52XW0UUsLbbZebn6/BLh9MFhzUDroN3+IpP5vwOa26FL9NhsxUh/8WzU183VLI
            KoJNEh5opbQ0s/oNnplsU2RCYPMtrs1YYS0NAOIjG6GQY6oh1CcBTx2ebtkFFAE4
            Q0m9sDkCgYEAzHkCxxDgbY1ViR2G9KvujC3bnG7uIDb24h+jZWatx78QpcIqp3yG
            I4ItfiGCmfFW89cX1NVooOuyRcpOrLw30PrRf0V8E/icTvFkdv3LtaZmsbTtRZbw
            osB7kuoYgemhvgu67ytg8scMqooSWLKNBj/wuwCEdwdRctkhAgG9vWcCgYEAx9UN
            rcim3cqEsxl/VsXG0dJLep2EPYHs88WXcfd9q4HttZs0vLDZVXglDbmvQJiPRjGk
            C6bMnZPy+a06ly6dMtgHmF0XVwgZQhlFSlIuM23/DiivAOOJ2qo+ES6+jvsBUojo
            euRrTi8JPwv5QFU+aN6/PaJ+vsnZrHaZHBdNDN0CgYAVQZs9UI7UNLYoq+4kr178
            KaRD7fBJXw1pUnqtBvCX7E/xu26tvK9BL75E93zZPhKZBMpQcOMQn5AH21E0edif
            nAN9ZJ7SgKzXNBcKm7W6q5LPdIyaCGf5s2LlUfq8Pqp21EdZp7vLYU/6xqHDoMQy
            WyFOf25F5XfdJZ9d0wqDjwKBgQCXpxCiekxotXDPmuIQsDeatMWjYDcjlp6EwceV
            LgWpSwljcU4shOnq+yrjp69gjmbtFm8wiH1weP9EjDqS0UVreJcLAlrcKcFBcHwt
            UwDM9wVBcY6eVhAgamKAF8F2MPdn84669O6afwe9WRDnycl7PNBVriQSFo2jXL4F
            m4lV4QKBgFTIYgUi4Td8rcMJY/mduO8E7YCjj/GxQprQhS0AYlEeqdG3SZnf4dge
            gQ6GM/yke4FNfQEkkKSCVYphEReXnUJ9Bp/7cRGwB8FqHUYxN3UaP9bFg9KYWpzV
            1P75ytKqMe+x5blIRYgvizXtoNrTPVoNBIqzWa7Lt1u/Yr7ZyioC
            -----END RSA PRIVATE KEY-----""";

        try (final Reader reader = new StringReader(privateKey)) {
            return new JwtCredentials(buildJWT(user, reader));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials: " + e.getMessage(), e);
        }
    }

    public static JwtCredentials credentials(String user, String privateKey) {
        try (final Reader reader = new StringReader(privateKey)) {
            return new JwtCredentials(buildJWT(user, reader));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException("Cannot build credentials: " + e.getMessage(), e);
        }
    }
}
