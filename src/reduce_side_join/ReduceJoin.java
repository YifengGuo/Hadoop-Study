package reduce_side_join;

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

/**
 * @author yifengguo
 */
public class ReduceJoin {
    /**
     * mapper for sales.txt
     */
    public static class SalesRecordMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t"); // eg: 001    9.99    2012-05-17
            // parts[0] is id
            context.write(new Text(parts[0]), new Text("sales\t"+parts[1])); // "sales" is to tell reducer
                                                                                   // the source of input is from
                                                                                   // which mapper, so is "accounts"
        }
    }

    public static class AccountsRecordMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t"); // eg 004	Nasser Hafez	Premium	2001-04-23

            context.write(new Text(parts[0]), new Text("accounts\t"+parts[1]));
        }
    }

    /**
     *  SalesMapper output format is id sale_amount, eg: 001    9.99
     *  AccountsMapper output format is id + name, eg: 004  Nasser Hafez
     *  The ReduceJoinReducer is receive the output from both mappers and reduce their output into the format like
     *  {name : count + total}
     *  count is the number of sale count, total is total amount of sales
     */
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "";
            double total = 0.0;
            int count = 0;

            for (Text t : values) { // receive output from both mappers
                String[] parts = t.toString().split("\t");
                if (parts[0].equals("sales")) { // if from SalesMapper
                    count++;
                    total += Float.parseFloat(parts[1]);
                } else if (parts[0].equals("accounts")) { // if from AccountsMapper
                    name = parts[1];
                }
            }

            String str = String.format("%d\t%f", count, total);
            context.write(new Text(name), new Text(str));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Hadoop 2.7 api
        // Job job = Job.getInstance(Configuration conf)
        Job job = Job.getInstance(conf, "Reduce Side Join");
        job.setJarByClass(ReduceJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // MultipleInputs allows several inputs and set certain input format and mapper to corresponding input
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, SalesRecordMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, AccountsRecordMapper.class);

        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        // f - the path to delete.
        // recursive - if path is a directory and set to true, the directory is
        // deleted else throws an exception. In case of a file the recursive can be set to either true or false.
        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
