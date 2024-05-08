package com.quiltdata.quiltcore.workflows;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a version of the configuration data.
 * The version consists of a major, minor, and patch number.
 */
public class ConfigDataVersion implements Comparable<ConfigDataVersion> {
    /**
     * The major version number.
     */
    public final int major;
    
    /**
     * The minor version number.
     */
    public final int minor;
    
    /**
     * The patch version number.
     */
    public final int patch;

    /**
     * Constructs a ConfigDataVersion object with the specified major, minor, and patch numbers.
     *
     * @param major The major version number.
     * @param minor The minor version number.
     * @param patch The patch version number.
     */
    public ConfigDataVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    /**
     * Parses a string representation of a version into a ConfigDataVersion object.
     * The string should be in the format "major.minor.patch".
     * If any part of the version is missing, it will be assumed as 0.
     *
     * @param s The string representation of the version.
     * @return The ConfigDataVersion object parsed from the string.
     */
    public static ConfigDataVersion parse(String s) {
        List<Integer> values =
            Stream.concat(
                Pattern.compile("\\.").splitAsStream(s).map(Integer::valueOf),
                Stream.of(0, 0)
            )
            .limit(3)
            .collect(Collectors.toList());
        return new ConfigDataVersion(values.get(0), values.get(1), values.get(2));
    }

    /**
     * Checks if this ConfigDataVersion object is equal to another object.
     * Two ConfigDataVersion objects are considered equal if their major, minor, and patch numbers are the same.
     *
     * @param o The object to compare.
     * @return true if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConfigDataVersion)) {
            return false;
        }
        ConfigDataVersion cdv = (ConfigDataVersion)o;
        return major == cdv.major && minor == cdv.minor && patch == cdv.patch;
    }

    /**
     * Compares this ConfigDataVersion object with another ConfigDataVersion object.
     * The comparison is based on the major, minor, and patch numbers.
     *
     * @param o The ConfigDataVersion object to compare.
     * @return A negative integer if this object is less than the other object,
     *         zero if they are equal, or a positive integer if this object is greater.
     */
    @Override
    public int compareTo(ConfigDataVersion o) {
        return
            major == o.major
            ? minor == o.minor
              ? patch - o.patch
              : minor - o.minor
            : major - o.major;
    }
}
