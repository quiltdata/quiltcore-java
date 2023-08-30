package com.quiltdata.quiltcore.workflows;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigDataVersion implements Comparable<ConfigDataVersion> {
    public final int major;
    public final int minor;
    public final int patch;

    public ConfigDataVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConfigDataVersion)) {
            return false;
        }
        ConfigDataVersion cdv = (ConfigDataVersion)o;
        return major == cdv.major && minor == cdv.minor && patch == cdv.patch;
    }

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
