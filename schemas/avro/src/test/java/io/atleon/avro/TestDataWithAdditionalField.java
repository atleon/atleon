package io.atleon.avro;

public class TestDataWithAdditionalField extends TestData {

    private String data3;

    private TestDataWithAdditionalField() {

    }

    public static TestDataWithAdditionalField fromTestData(TestData testData) {
        TestDataWithAdditionalField modified = new TestDataWithAdditionalField();
        modified.setData1(testData.getData1());
        modified.setData2(testData.getData2());
        modified.setData3("DATA 3");
        return modified;
    }

    public void setData3(String data3) {
        this.data3 = data3;
    }
}
