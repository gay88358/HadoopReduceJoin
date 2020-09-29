package hadoop.crossProduct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class RepartitionedMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static String tagRecordSeparator = "\t";

    public final void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Record record = createRecord(value);
        context.write(record.getGroupKey(), tagged(record));
    }

    private Text tagged(Record record) {
        return format(getTag(), record.getValueWithoutGroupKey());
    }

    private Text format(String tag, String record) {
        return new Text(tag + tagRecordSeparator + record);
    }

    protected abstract String getTag();

    protected abstract Record createRecord(Text plainRecord);

}
