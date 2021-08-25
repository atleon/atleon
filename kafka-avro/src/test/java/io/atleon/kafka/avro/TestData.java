package io.atleon.kafka.avro;

import java.util.Objects;

public class TestData {

    private String data1;

    private String data2;

    public static TestData create() {
        TestData data = new TestData();
        data.setData1("DATA 1");
        data.setData2("DATA 2");
        return data;
    }

    public String getData1() {
        return data1;
    }

    public void setData1(String data1) {
        this.data1 = data1;
    }

    public String getData2() {
        return data2;
    }

    public void setData2(String data2) {
        this.data2 = data2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestData testData = (TestData) o;
        return Objects.equals(data1, testData.data1) &&
            Objects.equals(data2, testData.data2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data1, data2);
    }
}
