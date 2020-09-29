package hadoop.reduceJoin;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;

public class DataJoin extends Configured implements Tool {

    public static class MapClass extends DataJoinMapperBase {

        protected Text generateInputTag(String inputFile) {
            System.out.println(inputFile);
            String datasource = inputFile.split("-")[0];
            return new Text(datasource);
        }

        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            String line = aRecord.getData().toString();
            String[] tokens = line.split(",");
            String groupKey = tokens[0];
            return new Text(groupKey);
        }

        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable taggedWritable = new TaggedWritable((Text) value);
            taggedWritable.setTag(this.inputTag);
            return taggedWritable;
        }
    }

    public static class Reduce extends DataJoinReducerBase {

        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2) return null;
            StringBuilder joinedStr = new StringBuilder();
            for (int i=0; i<values.length; i++) {
                if (i > 0) joinedStr.append(",");
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = tw.getData().toString();
                String[] tokens = line.split(",", 2);
                joinedStr.append(tokens[1]);
            }
            TaggedWritable taggedWritable = new TaggedWritable(new Text(joinedStr.toString()));
            taggedWritable.setTag((Text) tags[0]);
            return taggedWritable;
        }
    }

    public static class TaggedWritable extends TaggedMapOutput {

        private Writable data;

        TaggedWritable(Writable data) {
            this.tag = new Text("");
            this.data = data;
        }

        public Writable getData() {
            return data;
        }

        public void write(DataOutput out) throws IOException {
            this.tag.write(out);
            this.data.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            this.tag.readFields(in);
            this.data.readFields(in);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, DataJoin.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("DataJoin");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        job.set("mapred.textoutputformat.separator", ",");

        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("/Users/koushiken/Desktop/inverseOutput"));

        int res = ToolRunner.run(new Configuration(),
                new DataJoin(),
                new String[]{
                        "/Users/koushiken/Desktop/input/customer-123.txt",
                        "/Users/koushiken/Desktop/inverseOutput"
        });

        System.exit(res);
    }
}
