package hadoop.crossProduct;


import org.apache.hadoop.io.Text;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class TagRecordMap {
    private final Map<String, List<String>> tagRecordsMap = new TreeMap<>();
    private final String tagRecordSeparator;

    TagRecordMap(List<Text> tagRecords, String tagRecordSeparator) {
        this.tagRecordSeparator = tagRecordSeparator;
        buildTagRecordMap(tagRecords);
    }

    private void buildTagRecordMap(List<Text> tagRecords) {
        for (Text tagRecord: tagRecords) {
            String tag = extractTag(tagRecord);
            String record = extractRecord(tagRecord);
            if (tagRecordsMap.containsKey(tag)) {
                tagRecordsMap.get(tag).add(record);
            } else {
                List<String> result = new ArrayList<>();
                result.add(record);
                tagRecordsMap.put(tag, result);
            }
        }
    }


    TagRecordMap(Iterable<Text> tagRecords, String tagRecordSeparator) {
        this.tagRecordSeparator = tagRecordSeparator;
        for (Text tagRecord: tagRecords) {
            String tag = extractTag(tagRecord);
            String record = extractRecord(tagRecord);
            if (tagRecordsMap.containsKey(tag)) {
                tagRecordsMap.get(tag).add(record);
            } else {
                List<String> result = new ArrayList<>();
                result.add(record);
                tagRecordsMap.put(tag, result);
            }
        }
    }

    private String extractRecord(Text tagRecord) {
        String[] tagAndRecord = splitTagAndRecord(tagRecord);
        return tagAndRecord[1];
    }

    private String extractTag(Text tagRecord) {
        String[] tagAndRecord = splitTagAndRecord(tagRecord);
        return tagAndRecord[0];
    }

    private String[] splitTagAndRecord(Text tagRecord) {
        String plainTagRecord = tagRecord.toString();
        return plainTagRecord.split(tagRecordSeparator);
    }

    void enumerateCrossProductElements(Consumer<CrossProductElement> consumer) {
        getCrossProductElements()
                .forEach(consumer);
    }

    List<CrossProductElement> getCrossProductElements() {
        List<List<CrossProductElement>> elements = getAllCrossProductElements();
        return elements.stream()
                .reduce(TagRecordMap::crossProduct)
                .orElseThrow(() -> new RuntimeException("element is empty"));
    }

    static List<CrossProductElement> crossProduct(List<CrossProductElement> element1, List<CrossProductElement> element2) {
        List<CrossProductElement> result = new ArrayList<>();
        for (CrossProductElement customerProduct: element1) {
            for (CrossProductElement transactionProduct: element2) {
                result.add(customerProduct.crossProduct(transactionProduct));
            }
        }
        return result;
    }

    private List<List<CrossProductElement>> getAllCrossProductElements() {
        return tagRecordsMap.entrySet()
                .stream()
                .map(e -> createCrossProductElementsFor(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private List<CrossProductElement> createCrossProductElementsFor(String tag, List<String> records) {
        return records.stream()
                        .map(r -> new CrossProductElement(Arrays.asList(tag), Arrays.asList(r)))
                        .collect(Collectors.toList());
    }

    boolean containsKey(String tag) {
        return tagRecordsMap.containsKey(tag);
    }

    String getRecord(String tag, int index) {
        return tagRecordsMap.get(tag).get(index);
    }
}