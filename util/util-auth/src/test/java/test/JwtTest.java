package test;

import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class JwtTest {

    private static PublicKey publicKey;
    private static PrivateKey privateKey;

    @BeforeClass
    public static void beforeClass() throws Exception {
        var keys = RsaUtils.generateRsaKeys();
        publicKey = CredentialsUtils.readPublicKey(keys.publicKeyPath());
        privateKey = CredentialsUtils.readPrivateKey(keys.privateKeyPath());
    }

    @Test
    public void test() throws Exception {
        var now = Date.from(Instant.now());

        var jwt = JwtUtils.buildJWT("some_user", "provider", now, JwtUtils.afterDays(1), privateKey);

        Assert.assertTrue(JwtUtils.checkJWT(publicKey, jwt, "some_user", "provider"));
        Assert.assertFalse(JwtUtils.checkJWT(publicKey, jwt, "Superman", "provider"));
        Assert.assertFalse(JwtUtils.checkJWT(publicKey, jwt, "some_user", "another_provider"));
    }

    @Test
    public void testExpiredJwt() {
        var clock = new TestClock(0);

        var jwt = JwtUtils.buildJWT("user", "provider", Date.from(clock.instant()),
            Date.from(Instant.now(clock).plus(1, ChronoUnit.DAYS)), privateKey);

        Assert.assertTrue(JwtUtils.checkJWT(publicKey, jwt, "user", "provider", clock));

        clock.tick(Duration.ofHours(23));
        Assert.assertTrue(JwtUtils.checkJWT(publicKey, jwt, "user", "provider", clock));

        clock.tick(Duration.ofHours(2));
        Assert.assertFalse(JwtUtils.checkJWT(publicKey, jwt, "user", "provider", clock));
    }

    @Test
    public void testRenewableJwt() {
        var clock = new TestClock(0);

        var jwt = new RenewableJwt("user", "provider", Duration.ofDays(1), privateKey, clock);
        var creds = jwt.get();

        for (int i = 0; i < 23; ++i) {
            clock.tick(Duration.ofHours(1));
            Assert.assertTrue(JwtUtils.checkJWT(publicKey, creds.token(), "user", "provider", clock));
            Assert.assertSame(creds, jwt.get());
            Assert.assertEquals(1, jwt.getCount());
        }

        clock.tick(Duration.ofHours(2));
        Assert.assertNotSame(creds, jwt.get());
        Assert.assertFalse(JwtUtils.checkJWT(publicKey, creds.token(), "user", "provider", clock));
        creds = jwt.get();
        Assert.assertTrue(JwtUtils.checkJWT(publicKey, creds.token(), "user", "provider", clock));
        Assert.assertEquals(2, jwt.getCount());
    }

    private static final class TestClock extends Clock {
        private long seconds;

        public TestClock(long seconds) {
            this.seconds = seconds;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochSecond(seconds);
        }

        public void tick(Duration step) {
            seconds += step.getSeconds();
        }
    }
}
