package ru.yandex.cloud.ml.platform.lzy.server.utils.yc;

import java.util.Date;
import java.util.function.Supplier;


public class RenewableTokenInstance implements Supplier<String> {
    private static final long HOUR = 60 * 60 * 1000;
    private final Supplier<String> tokenSupplier;

    private Date lastTokenUpdateTime;
    private String cachedToken = null;

    public RenewableTokenInstance(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    public String getToken() {
        final Date now = new Date();
        if (cachedToken == null || now.getTime() - lastTokenUpdateTime.getTime() > HOUR) {
            lastTokenUpdateTime = now;
            cachedToken = tokenSupplier.get();
        }
        return cachedToken;
    }

    @Override
    public String get() {
        return getToken();
    }
}
