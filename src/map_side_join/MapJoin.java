package map_side_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.*;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * @author yifengguo
 */
public class MapJoin {

    public static class MapJoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public void close(){

        }
        private Map<String, String> joinData = new HashMap<>();
        @Override
        public void configure(JobConf job) {
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                setupMap(cacheFiles[0].toString());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        public void setupMap(String filename) throws IOException {
            Map<String, String> joinMap = new HashMap<>();
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split("\t");
                joinMap.put(data[0], data[1]);
            }
            joinData = joinMap;
            br.close();
        }
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) {
            String[] data = value.toString().split("\t"); // id + sale + date

            // output form: name + count + total_sale
            String outValue = String.format("%d\t%f", 1, Float.parseFloat(data[1]));
            Text out = new Text(outValue);
            try {
                collector.collect(new Text(joinData.get(data[0])), out); // key: name, value: count + sale
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MyReducer implements Reducer<Text, Text, Text, Text> {
        public void configure(JobConf job) {

        }

        public void close() {

        }

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) {
            // name + 1 + sale

            String name = "";
            float total = 0;
            int count = 0;
            while (values.hasNext()) {
                String[] data = values.next().toString().split("\t");
                count++;
                total += Float.parseFloat(data[1]);
            }
            String res = String.format("%d\t%f", count, total);
            try {
                collector.collect(new Text(key), new Text(res));
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        Configuration config = new Configuration();
        // initialize the driver
        JobConf conf = new JobConf(config, MapJoin.class);
        conf.setJobName(MapJoin.class.getName());
        DistributedCache.addLocalFiles(conf, "/home/yifengguo/IdeaProjects/Hadoop-Study/src/map_side_join/accounts.txt");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapJoinMapper.class);

        conf.setReducerClass(MyReducer.class);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
