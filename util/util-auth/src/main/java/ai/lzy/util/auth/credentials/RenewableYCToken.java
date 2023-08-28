package ai.lzy.util.auth.credentials;

import java.util.function.Supplier;

public class RenewableYCToken implements RenewableToken {

    private final Supplier<String> tokenSupplier;

    public RenewableYCToken(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public Credentials get() {
        return new YcIamCredentials(tokenSupplier.get());
    }
}
