package hadoop.crossProduct;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class CrossProductJoinTest {
    private final static String tagRecordSeparator = "\t";
    private final static String customerTag = "customer";
    private final static String customerRecord = "zixuanhong,26";
    private final static String transactionTag = "transaction";
    private final static String transactionRecord = "10,23";


    @Test
    public void tag_record_contains_tag_and_multiple_record() {
        Text customerTagRecord1 = getTagRecord(customerTag, customerRecord);
        Text customerTagRecord2 = getTagRecord(customerTag, customerRecord + customerRecord);
        TagRecordMap tagRecordMap = createTagRecordMap(customerTagRecord1, customerTagRecord2);

        assertEquals(tagRecordMap.getRecord(customerTag, 0), customerRecord);
        assertEquals(tagRecordMap.getRecord(customerTag, 1), customerRecord + customerRecord);
    }

    private TagRecordMap createTagRecordMap(Text ...tagRecords) {
        return new TagRecordMap(Arrays.asList(tagRecords), tagRecordSeparator);
    }

    @Test
    public void tag_record_contains_tag_and_record() {
        Text customerTagRecord = getTagRecord(customerTag, customerRecord);
        Text transactionTagRecord = getTagRecord(transactionTag, transactionRecord);
        TagRecordMap tagRecordMap = createTagRecordMap(transactionTagRecord, customerTagRecord);

        assertTrue(tagRecordMap.containsKey(customerTag));
        assertEquals(tagRecordMap.getRecord(customerTag, 0), customerRecord);

        assertTrue(tagRecordMap.containsKey(transactionTag));
        assertEquals(tagRecordMap.getRecord(transactionTag, 0), transactionRecord);
    }

    private Text getTagRecord(String customerTag, String customerRecord) {
        return new Text(customerTag + tagRecordSeparator + customerRecord);
    }

}