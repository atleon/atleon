package io.atleon.micrometer;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class MeterKey {

    private final String name;

    private final Tags tags;

    public MeterKey(String name, Map<String, String> tags) {
        this(
                name,
                tags.entrySet().stream()
                        .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList()));
    }

    public MeterKey(String name, Iterable<Tag> tags) {
        this(name, Tags.of(tags));
    }

    public MeterKey(String name, Tags tags) {
        this.name = name;
        this.tags = tags;
    }

    public MeterKey withNameQualifier(String qualifier) {
        return new MeterKey(name + "." + qualifier, tags);
    }

    public MeterKey withNameQualifierAndTag(String qualifier, String tagKey, String tagValue) {
        return new MeterKey(name + "." + qualifier, tags.and(tagKey, tagValue));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MeterKey meterKey = (MeterKey) o;
        return Objects.equals(name, meterKey.name) && Objects.equals(tags, meterKey.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags);
    }

    @Override
    public String toString() {
        return "MeterKey{name='" + name + "', tags=" + tags + "}";
    }

    public String getName() {
        return name;
    }

    public Iterable<Tag> getTags() {
        return tags;
    }
}
