package ai.lzy.util.grpc;

import ai.lzy.test.ValidationTest;
import ai.lzy.v1.util.LV;
import org.junit.Assert;
import org.junit.Test;

public class ProtoPrinterTest {

    @Test
    public void testSensitive() {
        var msg = ValidationTest.TestMessage.newBuilder()
            .setField1("field1")
            .setField2("field2")
            .setField3(1)
            .setField4(2)
            .build();

        var printer = ProtoPrinter.printer();
        Assert.assertEquals(
            "field1: \"field1\" field2: \"field2\" field3: 1 field4: 2",
            printer.shortDebugString(msg));

        printer = ProtoPrinter.printer().usingSensitiveExtension(LV.sensitive);
        Assert.assertEquals(
            "field1: \"field1\" field2: \"xxx\" field3: 1 field4: \"xxx\"",
            printer.shortDebugString(msg));
    }

}
