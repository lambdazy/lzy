package ai.lzy.util.auth.credentials;

import java.security.PrivateKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RenewableJwt implements RenewableToken {
    private static final long LIFETIME_GAP_SEC = Duration.ofMinutes(5).toSeconds();

    private final String issuer;
    private final String provider;
    private final Duration ttl;
    private final PrivateKey privateKey;
    private final Clock clock;
    private final AtomicReference<CurrentJwt> current = new AtomicReference<>();
    private final AtomicInteger count = new AtomicInteger(0);

    public RenewableJwt(String issuer, String provider, Duration ttl, PrivateKey privateKey) {
        this(issuer, provider, ttl, privateKey, Clock.systemUTC());
    }

    public RenewableJwt(String issuer, String provider, Duration ttl, PrivateKey privateKey, Clock clock) {
        this.issuer = issuer;
        this.provider = provider;
        this.ttl = ttl;
        this.privateKey = privateKey;
        this.clock = clock;
        current.set(createNewToken());
    }

    @Override
    public Credentials get() {
        var ts = now().getEpochSecond();
        var candidate = current.get();
        if (candidate.deadline.getEpochSecond() - ts > LIFETIME_GAP_SEC) {
            return candidate.jwt;
        }

        synchronized (this) {
            candidate = current.get();
            if (candidate.deadline.getEpochSecond() - ts > LIFETIME_GAP_SEC) {
                return candidate.jwt;
            }

            candidate = createNewToken();
            current.set(candidate);
            return candidate.jwt;
        }
    }

    public int getCount() {
        return count.get();
    }

    private CurrentJwt createNewToken() {
        var now = now();
        var expiresAt = Date.from(now.plus(ttl));
        count.getAndIncrement();
        return new CurrentJwt(
            new JwtCredentials(JwtUtils.buildJWT(issuer, provider, Date.from(now), expiresAt, privateKey)),
            expiresAt.toInstant());
    }

    private Instant now() {
        return clock.instant();
    }

    private record CurrentJwt(
        JwtCredentials jwt,
        Instant deadline
    ) {}
}
