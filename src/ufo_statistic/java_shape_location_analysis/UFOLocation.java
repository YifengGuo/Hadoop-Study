package ufo_statistic.java_shape_location_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yifengguo
 */
public class UFOLocation {
    /**
     * This mapper is to retrieve 2-letter sequence (record location) from input by Regex for many records are not
     * in right form
     */
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private static Pattern locationPattern = Pattern.compile("[a-zA-Z]{2}[^a-zA-Z]*$");

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] data = line.split("\t");
            String location = data[2].trim();

            if (location.length() >= 2) {
                Matcher matcher = locationPattern.matcher(location);
                if (matcher.find()) {
                    int start = matcher.start(); // return the start index of the previous match
                    String state = location.substring(start, start + 2);
                    output.collect(new Text(state.toUpperCase()), one);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        // initialize the driver
        JobConf conf = new JobConf(config, UFOLocation.class);
        conf.setJobName("UFOLocation");

        // final output format
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        JobConf mapconf1 = new JobConf(false);
        // ChainMapper.addMapper() parameter list:
        // job, mapper, inputKey, inputValue, outputKey, outputValue, byValue, mapperConf
        // add first mapper into ChainMapper
        // UFORecordValidationMapper output is <LongWritable, Text>
        ChainMapper.addMapper(conf, UFORecordValidationMapper.class, LongWritable.class, Text.class, LongWritable.class,
                Text.class, true, mapconf1);

        JobConf mapconf2 = new JobConf(false);
        // add second mapper into ChainMapper
        // MapClass output is <Text, LongWritable>
        ChainMapper.addMapper(conf, MapClass.class, LongWritable.class, Text.class, Text.class, LongWritable.class,
                true, mapconf2);

        // In ChainMapper:
        // 1. Except the last mapper, each previous mapper's output matches the next mapper's input in the chain
        // 2. the last mapper's output matches the input of the first reducer


        conf.setMapperClass(ChainMapper.class);
        conf.setCombinerClass(LongSumReducer.class);
        conf.setReducerClass(LongSumReducer.class);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        // the problem of this program is the output contains some abbreviations that are not the name of states
        // so we need to add something (an official full-tile -> abbreviation dict) to mapper so that the right
        // abbreviation can be retrieved
    }
}
