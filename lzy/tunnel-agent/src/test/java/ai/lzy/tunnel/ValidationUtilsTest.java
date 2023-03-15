package ai.lzy.tunnel;


import ai.lzy.tunnel.service.ValidationUtils;
import org.junit.Assert;
import org.junit.Test;

public class ValidationUtilsTest {
    @Test
    public void validateCIDR() {
        Assert.assertFalse(ValidationUtils.validateCIDR(""));
        Assert.assertFalse(ValidationUtils.validateCIDR("foo/bar"));
        Assert.assertFalse(ValidationUtils.validateCIDR("foo/1"));
        Assert.assertFalse(ValidationUtils.validateCIDR("google.com/1"));
        Assert.assertFalse(ValidationUtils.validateCIDR("256.1.1.1/1"));
        Assert.assertFalse(ValidationUtils.validateCIDR("1.1.1.1"));
        Assert.assertFalse(ValidationUtils.validateCIDR("fe80::"));
        Assert.assertFalse(ValidationUtils.validateCIDR("1.2.3.4/33"));
        Assert.assertFalse(ValidationUtils.validateCIDR("1.2.3.4//5"));

        Assert.assertTrue(ValidationUtils.validateCIDR("255.255.255.255/32"));
        Assert.assertTrue(ValidationUtils.validateCIDR("1.1.1.1/24"));
        Assert.assertTrue(ValidationUtils.validateCIDR("0.0.0.0/0"));
        Assert.assertTrue(ValidationUtils.validateCIDR("192.168.10.0/24"));
    }

    @Test
    public void validateIPv4() {
        Assert.assertFalse(ValidationUtils.validateIpV4("1231.2.3.1.2.3.311.6."));
        Assert.assertFalse(ValidationUtils.validateIpV4("foo.bar"));
        Assert.assertFalse(ValidationUtils.validateIpV4("1.2.3."));
        Assert.assertFalse(ValidationUtils.validateIpV4(".1.2.3"));
        Assert.assertFalse(ValidationUtils.validateIpV4("1.1.2.3."));
        Assert.assertFalse(ValidationUtils.validateIpV4("1.1.1.256"));
        Assert.assertFalse(ValidationUtils.validateIpV4("256.1.1.1"));
        Assert.assertFalse(ValidationUtils.validateIpV4("a.b.c.d"));

        Assert.assertTrue(ValidationUtils.validateIpV4("0.1.2.3"));
        Assert.assertTrue(ValidationUtils.validateIpV4("255.255.255.255"));
        Assert.assertTrue(ValidationUtils.validateIpV4("0.0.0.0"));
    }

    @Test
    public void validateIPv6() {
        Assert.assertFalse(ValidationUtils.validateIpV6("beba:::"));
        Assert.assertFalse(ValidationUtils.validateIpV6("beba:"));
        Assert.assertFalse(ValidationUtils.validateIpV6("geba::"));
        Assert.assertFalse(ValidationUtils.validateIpV6("ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        Assert.assertFalse(ValidationUtils.validateIpV6("x.y.z"));
        Assert.assertFalse(ValidationUtils.validateIpV6("fefefafaf"));

        Assert.assertTrue(ValidationUtils.validateIpV6("beba::"));
        Assert.assertTrue(ValidationUtils.validateIpV6("0000:0000:0000:0000:0000:0000:0000:0000"));
        Assert.assertTrue(ValidationUtils.validateIpV6("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        Assert.assertTrue(ValidationUtils.validateIpV6("1234:5678::"));
        Assert.assertTrue(ValidationUtils.validateIpV6("::eeee:eeee:cafe"));

    }
}
