package hadoop.crossProduct;

import org.apache.hadoop.io.Text;

import java.util.function.Predicate;

public class QAGaugeMapper extends RepartitionedMapper {
    @Override
    protected String getTag() {
        return "qaguage";
    }

    @Override
    protected Record createRecord(Text plainRecord) {
        return BounderRecord.createQAGaugeRecord(plainRecord);
    }

}
