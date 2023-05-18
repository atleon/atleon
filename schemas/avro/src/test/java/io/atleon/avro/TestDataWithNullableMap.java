package io.atleon.avro;

import org.apache.avro.reflect.Nullable;

import java.util.Map;

public class TestDataWithNullableMap {

    @Nullable
    private Map<String, TestData> map;

    public Map<String, TestData> getMap() {
        return map;
    }

    public void setMap(Map<String, TestData> map) {
        this.map = map;
    }
}
