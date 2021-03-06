package hadoop.reduceJoin;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CitationHistogram extends Configured implements Tool {

    public static class MapClass extends MapReduceBase
            implements Mapper<Text, Text, IntWritable, IntWritable> {

        private final static IntWritable uno = new IntWritable(1);
        private IntWritable citationCount = new IntWritable();

        public void map(Text patent, Text citationCount,
                        OutputCollector<IntWritable, IntWritable> output,
                        Reporter reporter) throws IOException {

            this.citationCount.set(Integer.parseInt(citationCount.toString()));
            output.collect(this.citationCount, uno);
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
    {

        public void reduce(IntWritable citationCount, Iterator<IntWritable> times,
                           OutputCollector<IntWritable, IntWritable>output,
                           Reporter reporter) throws IOException {

            int count = 0;
            while (times.hasNext()) {
                count += times.next().get();
            }
            output.collect(citationCount, new IntWritable(count));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, CitationHistogram.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("CitationHistogram");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("/Users/koushiken/Desktop/inverseOutput"));

        int res = ToolRunner.run(new Configuration(),
                new CitationHistogram(),
                new String[]{"/Users/koushiken/Desktop/input/NOTICE.txt", "/Users/koushiken/Desktop/inverseOutput"});

        System.exit(res);
    }
}
