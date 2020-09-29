package hadoop.crossProduct;

import org.apache.hadoop.io.Text;

public class MotionFeatureSecondBoundMapper extends RepartitionedMapper {
    @Override
    protected String getTag() {
        return "mfbs";
    }

    @Override
    protected Record createRecord(Text plainRecord) {
        return BounderRecord.createMotionFeatureSecondBoundRecord(plainRecord);
    }
}
