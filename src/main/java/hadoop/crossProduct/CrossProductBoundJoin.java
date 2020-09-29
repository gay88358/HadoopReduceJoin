package hadoop.crossProduct;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static java.util.stream.Collectors.joining;

public class CrossProductBoundJoin {
    private final static String tagRecordSeparator = "\t";
    private final static int MAPPER_SIZE = 4;

    public static class ReduceSideJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) {
            TagRecordMap tagRecordMap = new TagRecordMap(values, tagRecordSeparator);
            tagRecordMap.enumerateCrossProductElements(e -> writeInnerJoinRecord(e, context));
        }

        private void writeInnerJoinRecord(CrossProductElement element, Context context) {
            if (element.tagSize() != MAPPER_SIZE)
                return;

            String joinRecord = createJoinRecord(element);
            String firstTag = element.getTag(0);

            System.out.println(joinRecord);
            try {
                context.write(new Text(firstTag), new Text(joinRecord));
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private String createJoinRecord(CrossProductElement element) {
            return element.combine(
                    (tags, records) ->
                            records
                                    .stream()
                                    .collect(
                                            joining(",")
                                    )
            );
        }
    }

    public static void main(String[] args) throws Exception {
        PathHolder pathHolder = new PathHolder(args);
        Configuration conf = new Configuration();
        Job job = createJob(conf);
        addMapper(job, pathHolder.mfbbFilePath(), MotionFeatureFirstBoundMapper.class);
        addMapper(job, pathHolder.mfbsFilePath(), MotionFeatureSecondBoundMapper.class);
        addMapper(job, pathHolder.qaScopeFilePath(), QAScopeMapper.class);
        addMapper(job, pathHolder.qaGaugeFilePath(), QAGaugeMapper.class);

        setUpFileOutputFormat(conf, job, pathHolder);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void setUpFileOutputFormat(Configuration conf, Job job, PathHolder pathHolder) throws IOException {
        Path outputPath = pathHolder.outputPath();
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
    }

    private static void addMapper(Job job, Path path, Class<? extends Mapper> mapperClass) {
        MultipleInputs.addInputPath(job, path, TextInputFormat.class, mapperClass);
    }

    private static Job createJob(Configuration conf) {
        try {
            Job job = new Job(conf, "Reduce-side join");
            job.setJarByClass(CrossProductBoundJoin.class);
            job.setReducerClass(ReduceSideJoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            return job;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}