package ai.lzy.iam.config;

import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public final class IamClientConfiguration {
    private String address;
    private String internalUserName;
    private String internalUserPrivateKey;

    public JwtCredentials createCredentials(Clock clock) {
        var from = Instant.now(clock);
        var till = from.plus(7, ChronoUnit.DAYS);
        return JwtUtils.credentials(internalUserName, AuthProvider.INTERNAL.name(), Date.from(from), Date.from(till),
            internalUserPrivateKey);
    }

    public JwtCredentials createCredentials() {
        return createCredentials(Clock.systemUTC());
    }

    public RenewableJwt createRenewableToken(Clock clock) {
        try {
            return new RenewableJwt(internalUserName, AuthProvider.INTERNAL.name(), Duration.ofDays(1),
                CredentialsUtils.readPrivateKey(internalUserPrivateKey), clock);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public RenewableJwt createRenewableToken() {
        return createRenewableToken(Clock.systemUTC());
    }

    public String getAddress() {
        return address;
    }

    public String getInternalUserName() {
        return internalUserName;
    }

    public String getInternalUserPrivateKey() {
        return internalUserPrivateKey;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setInternalUserName(String internalUserName) {
        this.internalUserName = internalUserName;
    }

    public void setInternalUserPrivateKey(String internalUserPrivateKey) {
        this.internalUserPrivateKey = internalUserPrivateKey;
    }
}
