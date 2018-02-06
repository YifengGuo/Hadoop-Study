package word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount1 {
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        // IntWritable object one is initialized as static variable so we can reuse it to set the word in mapper
        private final static IntWritable one = new IntWritable(1);
        // only initialize Text object word once and each time when it is called by function, reset it
        // the idea in initializing the IntWritable and Text object is to save the resource and relieve the impact of GC
        private Text word = new Text();

        /**
         * the output of mapper is a sequence of pairs (word, 1), word is the key and 1 is the value
         * the output will not be transmitted to Reducer immediately but need go through a process called shuffle
         * @param key by default is the line number of text but does not matter in word_count
         * @param value the content of text in a line
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");

            for ( String str : words) {
                word.set(str);
                context.write(word, one); // context.write will do no change to Object one
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * the output of Reducer is a sequence of pairs (word, count)
         * @param key key (word) after shuffle (calculating hashCode)
         * @param values the count
         * @param context result pair for each word
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable val : values) {
                total++;
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount1.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
