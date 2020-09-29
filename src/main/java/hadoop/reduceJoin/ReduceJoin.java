package hadoop.reduceJoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
    public static class CustomerMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            CustomerRecord record = new CustomerRecord(value);
            context.write(new Text(record.customerId()), new Text("cust   " + record.customerName()));
        }

        private static class CustomerRecord {
            private final String[] parts;
            CustomerRecord(Text value) {
                String record = value.toString();
                parts = record.split(",");
            }

            Text customerId() {
                return new Text(parts[0]);
            }

            String customerName() {
                return parts[1];
            }
        }
    }

    public static class TransactionMapper extends Mapper <Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            TransactionRecord record = new TransactionRecord(value);
            context.write(record.customerId(), new Text("tnxn   " + record.amount()));
        }

        private static class TransactionRecord {
            private final String[] parts;
            TransactionRecord(Text value) {
                String record = value.toString();
                parts = record.split(",");
            }

            Text customerId() {
                return new Text(parts[2]);
            }

            String amount() {
                return parts[3];
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String name = "";
            double total = 0.0;
            int count = 0;

            for (Text t : values)
            {
                String parts[] = t.toString().split("  ");
                if (isTransactionRecord(parts))
                {
                    count++;
                    total += Float.parseFloat(parts[1]);
                }
                else if (isCustomerRecord(parts))
                {
                    name = parts[1];
                }
            }
            String str = String.format("%d %f", count, total);
            context.write(new Text(name), new Text(str));
        }

        private boolean isTransactionRecord(String parts[]) {
            return parts[0].equals("tnxn");
        }

        private boolean isCustomerRecord(String parts[]) {
            return parts[0].equals("cust");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = createJob(conf);
        MultipleInputs.addInputPath(job, customerFilePath(), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, customerTransactionFilePath(), TextInputFormat.class, TransactionMapper.class);
        Path outputPath = outputPath();
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Job createJob(Configuration conf) {
        Job job;
        try {
            job = new Job(conf, "Reduce-side join");
            job.setJarByClass(ReduceJoin.class);
            job.setReducerClass(ReduceJoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return job;
    }

    private static Path outputPath() {
        return pathOf("/Users/koushiken/Desktop/inverseOutput/");
    }

    private static Path customerFilePath() {
        return pathOf("/Users/koushiken/Desktop/input/customer.txt");
    }

    private static Path customerTransactionFilePath() {
        return pathOf("/Users/koushiken/Desktop/input/customerTransaction.txt");
    }

    private static Path pathOf(String dir) {
        return new Path(dir);
    }
}