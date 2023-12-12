package ai.lzy.util.grpc;

import ai.lzy.test.ValidationTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ProtoPrinterTest {

    @Test
    public void testSensitive() {
        var msg = ValidationTest.TestMessage.newBuilder()
            .setField1("field1")
            .setField2("field2")
            .setField3(1)
            .setField4(2)
            .putAllSecrets(Map.of("key1", "value1", "key2", "value2"))
            .putAllBadSecrets(Map.of(1L, "password"))
            .build();

        var printer = ProtoPrinter.printer();
        Assert.assertEquals(
            "field1: \"field1\" field2: \"field2\" field3: 1 field4: 2 " +
                "secrets { key: \"key1\" value: \"value1\" } secrets { key: \"key2\" value: \"value2\" } " +
                "bad_secrets { key: 1 value: \"password\" }",
            printer.shortDebugString(msg));

        printer = ProtoPrinter.safePrinter();
        Assert.assertEquals(
            "field1: \"field1\" field2: \"xxx\" field3: 1 field4: \"xxx\" " +
                "secrets { key: \"key1\" value: \"xxx\" } secrets { key: \"key2\" value: \"xxx\" } " +
                "bad_secrets { \"xxx\" }",
            printer.shortDebugString(msg));
    }

}
