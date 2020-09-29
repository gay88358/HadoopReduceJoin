package hadoop.crossProduct;

import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.stream.Stream;

abstract class Record {
    private final String[] parts;

    Record(Text value) {
        String record = value.toString();
        parts = record.split(",");
    }

    public abstract Text getGroupKey();

    public abstract String getValueWithoutGroupKey();

    String getByIndex(int index) {
        return parts[index];
    }

    Stream<String> recordStream() {
        return Arrays.asList(parts)
                .stream();
    }
}
