package io.atleon.avro;

public class StringDataHolder extends GenericDataHolder<String> {

    public static StringDataHolder create() {
        StringDataHolder stringDataHolder = new StringDataHolder();
        stringDataHolder.setData("data");
        return stringDataHolder;
    }
}
