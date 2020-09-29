package hadoop.crossProduct;

import org.apache.hadoop.io.Text;
import org.junit.Test;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;

public class CrossProductTest {
    private final static String tagRecordSeparator = "\t";
    private final static String customerTag = "customer";
    private final static String customerRecord1 = "zixuanhong,26";
    private final static String customerRecord2 = "kris,26";
    private final static String transactionTag = "transaction";
    private final static String transactionRecord1 = "10,23";
    private final static String transactionRecord2 = "10,23";

    @Test
    public void combine_tag_and_record_in_the_cross_product_element() {
        CrossProductElement crossProductElement = new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord1, transactionRecord1));

        String joiningRecord = crossProductElement
                        .combine((tags, records) ->
                                records.stream()
                                        .collect(
                                                Collectors.joining("")
                                        )
                        );

        assertEquals(joiningRecord, customerRecord1 + transactionRecord1);
    }

    private List<String> tags(String ...tags) {
        return Arrays.asList(tags);
    }

    private List<String> records(String ...records) {
        return Arrays.asList(records);
    }

    @Test
    public void merge_two_list_of_cross_product_element() {
        List<CrossProductElement> customerProduct =  Arrays.asList(
                createCrossProductElement(customerTag, customerRecord1)
        );
        List<CrossProductElement> transactionProduct = Arrays.asList(
                createCrossProductElement(transactionTag, transactionRecord1),
                createCrossProductElement(transactionTag, transactionRecord2)
        );

        List<CrossProductElement> result = TagRecordMap.crossProduct(customerProduct, transactionProduct);

        assertEquals(result.get(0), new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord1, transactionRecord1)));
        assertEquals(result.get(1), new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord1, transactionRecord2)));
    }

    private CrossProductElement createCrossProductElement(String tag, String record) {
        return new CrossProductElement(tags(tag), records(record));
    }

    @Test
    public void cross_product() {
        TagRecordMap tagRecordMap = createTagRecordMap();

        List<CrossProductElement> elements = tagRecordMap.getCrossProductElements();

        assertEquals(elements.size(), 4);
        assertEquals(elements.get(0), new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord1, transactionRecord1)));
        assertEquals(elements.get(1), new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord1, transactionRecord2)));
        assertEquals(elements.get(2), new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord2, transactionRecord1)));
        assertEquals(elements.get(3), new CrossProductElement(tags(customerTag, transactionTag), records(customerRecord2, transactionRecord2)));
    }

    private TagRecordMap createTagRecordMap() {
        Text cTagRecord1 = format(customerTag, customerRecord1);
        Text cTagRecord2 = format(customerTag, customerRecord2);
        Text tTagRecord1 = format(transactionTag, transactionRecord1);
        Text tTagRecord2 = format(transactionTag, transactionRecord2);
        return new TagRecordMap(Arrays.asList(cTagRecord1, cTagRecord2, tTagRecord1, tTagRecord2), tagRecordSeparator);
    }

    private Text format(String tag, String record) {
        return new Text(tag + tagRecordSeparator + record);
    }
}
