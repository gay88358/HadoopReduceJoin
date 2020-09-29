package hadoop.crossProduct;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrossProductElement {
    private List<String> tags;
    private List<String> records;

    CrossProductElement(List<String> tags, List<String> records) {
        this.tags = tags;
        this.records = records;
    }

    interface CombineStrategy {
        String combine(List<String> tags, List<String> records);
    }

    String combine(CombineStrategy strategy) {
        return strategy.combine(this.tags, this.records);
    }

    CrossProductElement crossProduct(CrossProductElement crossProductElement) {
        return new CrossProductElement(concat(this.tags, crossProductElement.tags), concat(this.records, crossProductElement.records));
    }

    private static List<String> concat(List<String> l1, List<String> l2) {
        return Stream
                .concat(l1.stream(), l2.stream())
                .collect(Collectors.toList());
    }

    String getTag(int index) {
        return tags.get(index);
    }

    int tagSize() {
        return tags.size();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        CrossProductElement that = (CrossProductElement) object;
        return Objects.equals(tags, that.tags) &&
                Objects.equals(records, that.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tags, records);
    }
}
