package io.atleon.avro;

import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

public class TestDataWithProblematicTypes {

    private Set<String> dataSet;

    private SortedSet<String> sortedDataSet;

    public Set<String> getDataSet() {
        return dataSet;
    }

    public void setDataSet(Set<String> dataSet) {
        this.dataSet = dataSet;
    }

    public SortedSet<String> getSortedDataSet() {
        return sortedDataSet;
    }

    public void setSortedDataSet(SortedSet<String> sortedDataSet) {
        this.sortedDataSet = sortedDataSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestDataWithProblematicTypes that = (TestDataWithProblematicTypes) o;
        return Objects.equals(dataSet, that.dataSet) && Objects.equals(sortedDataSet, that.sortedDataSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSet, sortedDataSet);
    }
}
