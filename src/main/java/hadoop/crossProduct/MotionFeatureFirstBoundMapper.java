package hadoop.crossProduct;

import org.apache.hadoop.io.Text;

public class MotionFeatureFirstBoundMapper extends RepartitionedMapper {
    @Override
    protected String getTag() {
        return "mfbb";
    }

    @Override
    protected Record createRecord(Text plainRecord) {
        return BounderRecord.createMotionFeatureFirstBoundRecord(plainRecord);
    }
}
