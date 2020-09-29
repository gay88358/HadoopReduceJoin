package hadoop.crossProduct;

import org.apache.hadoop.io.Text;

import java.util.function.Predicate;

import static java.util.stream.Collectors.joining;

public class BounderRecord extends Record {
    private String LOT_ID;
    private String LF_ID;
    private String UNIT_NO;
    private String ROW_NO;
    private String COL_NO;
    private String WIRE_NO;

    static BounderRecord createMotionFeatureSecondBoundRecord(Text value) {
        return new BounderRecord(value);
    }

    static BounderRecord createMotionFeatureFirstBoundRecord(Text value) {
        return new BounderRecord(value);
    }

    static BounderRecord createQAGaugeRecord(Text value) {
        return new BounderRecord(value);
    }

    static BounderRecord createQAScopeRecord(Text value) {
        return new BounderRecord(value);
    }

    private BounderRecord(Text value) {
        super(value);
        LOT_ID = getByIndex(4);
        LF_ID = getByIndex(6);
        UNIT_NO = getByIndex(8);
        ROW_NO = getByIndex(9);
        COL_NO = getByIndex(10);
        WIRE_NO = getByIndex(12);

    }

    @Override
    public String getValueWithoutGroupKey() {
        return recordStream()
                .filter(ordinaryCell())
                .collect(joining(","));
    }

    @Override
    public Text getGroupKey() {
        return new Text(LOT_ID + LF_ID + UNIT_NO + ROW_NO + COL_NO + WIRE_NO);
    }

    private Predicate<String> ordinaryCell() {
        return cell ->
                !cell.equals(LOT_ID)
                        && !cell.equals(LF_ID)
                        && !cell.equals(UNIT_NO)
                        && !cell.equals(ROW_NO)
                        && !cell.equals(COL_NO)
                        && !cell.equals(WIRE_NO);
    }
}
