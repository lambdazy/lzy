package ru.yandex.qe.s3.util;

import jakarta.annotation.Nonnull;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

/**
 * Formatting utilities that output file timestamps, event duration, and file sizes in a simple human-readable format
 * inspired by the {@code ls} utility.
 *
 * @author entropia
 */
public final class HumanReadable {

    /**
     * Default human-readable format for date & time, similar to one used by the {@code ls} command line utility: {@code
     * Jan 1, 2015 14:32}
     *
     * @see #dateTime(Instant)
     * @see #dateTime(org.joda.time.Instant)
     */
    public static final String LS_DATE_TIME_PATTERN = "MMM d, yyyy HH:mm";

    /**
     * Default period formatter, which outputs time periods in the {@code HH:mm:ss.ms} format.
     */
    public static final PeriodFormatter DEFAULT_PERIOD_FORMATTER = new PeriodFormatterBuilder()
        .printZeroAlways()
        .minimumPrintedDigits(2)
        .appendHours()
        .appendSuffix(":")
        .appendMinutes()
        .appendSuffix(":")
        .appendSeconds()
        .appendSuffix(".")
        .appendMillis3Digit()
        .toFormatter();
    private static final String[] SIZE_SUFFIXES = new String[] {"", "K", "M", "G", "T", "P", "E", "Z", "Y"};
    private static final BigInteger _1024 = BigInteger.valueOf(1024);
    private static final BigInteger[] POW1024 = new BigInteger[SIZE_SUFFIXES.length];
    private static final BigDecimal[] D_POW1024 = new BigDecimal[POW1024.length];
    private static final Map<TimeUnit, String> TRUNCATING_TIME_UNITS;

    static {
        BigInteger last = BigInteger.ONE;
        POW1024[0] = last;
        for (int i = 1; i < POW1024.length; i++) {
            last = last.multiply(_1024);
            POW1024[i] = last;
        }

        for (int i = 0; i < D_POW1024.length; i++) {
            D_POW1024[i] = new BigDecimal(POW1024[i]);
        }
    }

    static {
        final LinkedHashMap<TimeUnit, String> map = new LinkedHashMap<>(3);
        map.put(NANOSECONDS, "ns");
        map.put(MICROSECONDS, "us");
        map.put(MILLISECONDS, "ms");
        TRUNCATING_TIME_UNITS = Collections.unmodifiableMap(map);
    }

    private HumanReadable() {
    }

    /**
     * Returns human-readable string describing file size (e.g., "150" for 150 bytes, "28K" for 28 kilobytes, "56G" for
     * 56 gigabytes, and so on.
     *
     * @param size file size in bytes
     * @return human-readable file size string
     * @throws IllegalArgumentException {@code size < 0}
     */
    @Nonnull
    public static String fileSize(long size) {
        return fileSize(BigInteger.valueOf(size));
    }

    /**
     * Returns human-readable string describing file size (e.g., "150" for 150 bytes, "28K" for 28 kilobytes, "56G" for
     * 56 gigabytes, and so on.
     *
     * @param size file size in bytes
     * @return human-readable file size string
     * @throws IllegalArgumentException {@code size < 0}
     * @throws NullPointerException     {@code size == null}
     */
    @Nonnull
    public static String fileSize(@Nonnull BigInteger size) {
        if (size.compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("size must not be < 0");
        }

        int log1024 = log1024(size);

        final String units = SIZE_SUFFIXES[log1024];
        final BigDecimal sizeInUnits = new BigDecimal(size)
            .setScale(2, BigDecimal.ROUND_HALF_UP)
            .divide(D_POW1024[log1024], BigDecimal.ROUND_HALF_UP);

        final DecimalFormat fmt = new DecimalFormat("#0.##");
        fmt.setDecimalFormatSymbols(DecimalFormatSymbols.getInstance(Locale.US));
        return fmt.format(sizeInUnits) + units;
    }

    private static int log1024(BigInteger value) {
        int pow = 0;
        for (int i = 0; i < POW1024.length; i++) {
            BigInteger start = POW1024[i];
            BigInteger end = (i == POW1024.length - 1 ? null : POW1024[i + 1]);

            if (value.compareTo(start) >= 0
                && (end == null || value.compareTo(end) < 0)) {
                pow = i;
                break;
            }
        }

        return pow;
    }

    /**
     * @param instant instant in time
     * @return date and time appropriately formatted using the current time zone, e.g. {@code Jan 1, 2015 00:01}
     * @see #LS_DATE_TIME_PATTERN
     */
    @Nonnull
    public static String dateTime(@Nonnull Instant instant) {
        return dateTime(instant, LS_DATE_TIME_PATTERN);
    }

    /**
     * @param instant instant in time
     * @param pattern date & time format pattern
     * @return date & time formatted using the current time zone and format pattern
     * @throws IllegalArgumentException    illegal format pattern
     * @throws java.time.DateTimeException could not format date & time
     */
    @Nonnull
    public static String dateTime(@Nonnull Instant instant, @Nonnull String pattern) {
        return DateTimeFormatter
            .ofPattern(pattern, Locale.US)
            .format(ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

    /**
     * @param instant instant in time
     * @return date and time appropriately formatted using the current time zone, e.g. {@code Jan 1, 2015 00:01}
     * @see #LS_DATE_TIME_PATTERN
     */
    @Nonnull
    public static String dateTime(@Nonnull org.joda.time.Instant instant) {
        return dateTime(instant, LS_DATE_TIME_PATTERN);
    }

    /**
     * @param instant instant in time
     * @param pattern date & time format pattern
     * @return date & time formatted using the current time zone and format pattern
     */
    @Nonnull
    public static String dateTime(@Nonnull org.joda.time.Instant instant, @Nonnull String pattern) {
        return DateTimeFormat
            .forPattern(pattern)
            .withLocale(Locale.US)
            .withZone(DateTimeZone.getDefault())
            .print(instant);
    }

    /**
     * Converts a duration of time expressed in nanoseconds into a human-readable representation, e.g. 999 ns into
     * "999ns", 1001 ns into "1us".
     *
     * @param duration duration of time in nanoseconds
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    public static String durationNanos(long duration) {
        return duration(duration, NANOSECONDS);
    }

    /**
     * Converts a duration of time expressed in microseconds into a human-readable representation, e.g. 999 us into
     * "999us", 1001 us into "1ms".
     *
     * @param duration duration of time in microseconds
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    @Nonnull
    public static String durationMicros(long duration) {
        return duration(duration, MICROSECONDS);
    }

    /**
     * Converts a duration of time expressed in milliseconds into a human-readable representation, e.g. 150 ms into
     * "150ms", 63000 ms into "00:01:03.000", 3675025 ms into "01:01:15.025", 3615000 ms into "01:00:15.000".
     *
     * @param duration duration of time in milliseconds
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    @Nonnull
    public static String durationMillis(long duration) {
        return duration(duration, MILLISECONDS);
    }

    /**
     * Converts a duration of time expressed in seconds into a human-readable representation, e.g. 5 seconds into
     * "00:00:05.000", 63 seconds into "00:01:03.000".
     *
     * @param duration duration of time in seconds
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    @Nonnull
    public static String durationSeconds(long duration) {
        return duration(duration, SECONDS);
    }

    /**
     * Converts a duration of time expressed in minutes into a human-readable representation, e.g. 65 minutes into
     * "01:05:00.000".
     *
     * @param duration duration of time in minutes
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    @Nonnull
    public static String durationMinutes(long duration) {
        return duration(duration, MINUTES);
    }

    /**
     * Converts a duration of time expressed in hours into a human-readable representation, e.g. 3 hours into
     * "03:00:00.000".
     *
     * @param duration duration of time in hours
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    @Nonnull
    public static String durationHours(long duration) {
        return duration(duration, HOURS);
    }

    /**
     * Converts a duration of time expressed in days into a human-readable representation, e.g. 2 days into
     * "48:00:00.000".
     *
     * @param duration duration of time in days
     * @return human-readable representation of the time duration
     * @see #duration(long, TimeUnit)
     */
    @Nonnull
    public static String durationDays(long duration) {
        return duration(duration, DAYS);
    }

    /**
     * Converts a duration of time into a human-readable representation, e.g. 999 ns into "999ns", 1001 ns into "1us",
     * 150 ms into "150ms", 63000 ms into "00:01:03.000", 65 min into "01:05:00.000", 3675025 ms into "01:01:15.025",
     * 3615000 ms into "01:00:15.000".
     * <p>
     * Small time units (ns, us, ms) are truncated to the largest unit. Ordinary time units (s, min, h, d) are formatted
     * as {@code HH:mm:ss.SSS} (hours, minutes, seconds and microseconds).
     *
     * @param duration       duration of time
     * @param sourceTimeUnit time unit used
     * @return human-readable representation of the time duration
     */
    @Nonnull
    public static String duration(long duration, @Nonnull TimeUnit sourceTimeUnit) {
        return duration(duration, sourceTimeUnit, DEFAULT_PERIOD_FORMATTER);
    }

    /**
     * Converts a duration of time into a human-readable representation, e.g. 999 ns into "999ns", 1001 ns into "1us",
     * 150 ms into "150ms", 63000 ms into "00:01:03.000", 65 min into "01:05:00.000", 3675025 ms into "01:01:15.025",
     * 3615000 ms into "01:00:15.000".
     * <p>
     * Small time units (ns, us, ms) are truncated to the largest unit. Ordinary time units (s, min, h, d) are formatted
     * as {@code HH:mm:ss.SSS} (hours, minutes, seconds and microseconds).
     *
     * @param duration       duration of time
     * @param sourceTimeUnit time unit used
     * @param formatter      period formatter used for ordinary time units (seconds, minutes, hours and days)
     * @return human-readable representation of the time duration
     * @see #DEFAULT_PERIOD_FORMATTER
     */
    @Nonnull
    public static String duration(long duration, @Nonnull TimeUnit sourceTimeUnit,
        @Nonnull PeriodFormatter formatter) {
        final TimeUnit[] units = TimeUnit.values();

        // find the largest unit which can still represent the duration
        TimeUnit target = units[0];
        for (int i = 0; i < units.length; i++) {
            if (units[i].convert(duration, sourceTimeUnit) == 0) {
                // duration is less than 1 units[i]
                break;
            }
            target = units[i];
        }

        if (TRUNCATING_TIME_UNITS.containsKey(target)) {
            return String.format("%d%s", target.convert(duration, sourceTimeUnit), TRUNCATING_TIME_UNITS.get(target));
        } else {
            return formatter.print(Duration.millis(sourceTimeUnit.toMillis(duration)).toPeriod());
        }
    }
}
