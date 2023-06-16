package ai.lzy.tunnel.service;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public final class ValidationUtils {

    public static final Pattern IPV4_APPROXIMATE_PATTERN = Pattern.compile("(\\d{1,3}\\.){3}\\d{1,3}");

    private ValidationUtils() { }

    public static boolean validateCIDR(String cidr) {
        String[] podCidrParts = cidr.split("/");
        if (podCidrParts.length != 2) {
            return false;
        }
        String addressPart = podCidrParts[0];
        String maskPart = podCidrParts[1];
        if (!IPV4_APPROXIMATE_PATTERN.matcher(addressPart).matches()) {
            return false;
        }
        try {
            InetAddress address = InetAddress.getByName(addressPart);
            if (!(address instanceof Inet4Address)) {
                return false;
            }
            int mask = Integer.parseInt(maskPart);
            if (mask < 0 || mask > 32) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean validateIpV6(String ipv6) {
        try {
            InetAddress remoteAddress = InetAddress.getByName(ipv6);
            if (!(remoteAddress instanceof Inet6Address)) {
                return false;
            }
        } catch (UnknownHostException e) {
            return false;
        }
        return true;
    }

    public static boolean validateIpV4(String ipv4) {
        try {
            InetAddress remoteAddress = InetAddress.getByName(ipv4);
            if (!(remoteAddress instanceof Inet4Address)) {
                return false;
            }
        } catch (UnknownHostException e) {
            return false;
        }
        return true;
    }

}
