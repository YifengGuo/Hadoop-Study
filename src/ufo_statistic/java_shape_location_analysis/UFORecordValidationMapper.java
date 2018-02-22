package ufo_statistic.java_shape_location_analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author yifengguo
 * A simple mapper to check if current line is a valid record which has 6 columns
 */
public class UFORecordValidationMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>{
    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws
            IOException {
        String line = value.toString();
        if (validate(line)) {
            output.collect(key, value);
        }
    }

    private boolean validate(String s) {
        String[] data = s.split("\t");
        if (data.length != 6) { // check if current input is valid or not
            return false;
        }
        return true;
    }
}
