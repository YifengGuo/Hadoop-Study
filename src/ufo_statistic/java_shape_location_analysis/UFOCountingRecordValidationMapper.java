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
 */
public class UFOCountingRecordValidationMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,
        Text>{

    public enum LineCounters {
        BAD_LINES,
        TOO_MANY_TABS,
        TOO_FEW_TABS;

    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws
            IOException {
        String line = value.toString();

        if (isValid(line, reporter)) {
            output.collect(key, value);
        }
    }

    /**
     * determine if current input is valid (which has 6 columns) from ufo.tsv
     * meanwhile, record the bad lines types and its count using reporter and ENUM above
     * @param line
     * @param reporter
     * @return
     */
    private boolean isValid(String line, Reporter reporter) {
        String[] data = line.split("\t");
        if (data.length != 6) {
            if (data.length < 6) {
                reporter.incrCounter(LineCounters.TOO_FEW_TABS, 1);
            } else {
                reporter.incrCounter(LineCounters.TOO_MANY_TABS, 1);
            }
            reporter.incrCounter(LineCounters.BAD_LINES, 1);

            if ((reporter.getCounter(LineCounters.BAD_LINES).getCounter() % 10) == 0) {
                reporter.setStatus("Got 10 bad lines.");
                System.err.println("Read another 10 bad lines.");
            }
            return false;
        }
        return true;
    }
}
