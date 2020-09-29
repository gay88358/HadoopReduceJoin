package hadoop.crossProduct;

import org.apache.hadoop.io.Text;

public class QAScopeMapper extends RepartitionedMapper {

    @Override
    protected String getTag() {
        return "QSScope";
    }

    @Override
    protected Record createRecord(Text plainRecord) {
        return BounderRecord.createQAScopeRecord(plainRecord);
    }
}
