package io.atleon.avro;

import java.util.Map;
import org.apache.avro.reflect.Nullable;

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
