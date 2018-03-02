package graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * @author yifengguo
 * This class is to apply MapReduce to calculate distance to start node of each node in the graph
 * <p>
 * The representation of graph:
 * 1   2,3,4   0   C
 * 2   1,4
 * 3   1,5,6
 * 4   1,2
 * 5   3,6
 * 6   3,5
 * 7   6
 * <p>
 * each node has 4 columns:
 * 1. id
 * 2. neighbors separated by comma
 * 3. distance to the start node
 * 4. current state:
 * a. Done  ---> has been processed already              D
 * b. Currently Processing  ---> being processed now     C
 * c. Pending ---> need to process                       P
 */
public class GraphPath {
    // class of graph node
    public static class Node {
        private String id;
        private String neighbors;
        private int distance;
        private String state;

        // Constructor of Node which is to parse the text representation into a Node object
        Node(Text t) {
            String[] parts = t.toString().split("\t");
            this.id = parts[0];
            this.neighbors = parts[1];
            // if distance column is null or "", initialize it as -1
            if (parts.length < 3 || parts[2].equals("")) {
                this.distance = -1;
            } else {
                this.distance = Integer.valueOf(parts[2]);
            }
            // if state column is null or "", initialize it as P
            if (parts.length < 4 || parts[3].equals("")) {
                this.state = "P";
            } else {
                this.state = parts[3];
            }
        }

        // Constructor 2
        // create a Node given a key and value pair
        // convert the key value pair into a whole Text object and invoke Constructor 1
        Node(Text key, Text value) {
            this(new Text(key.toString() + "\t" + value.toString()));
        }

        // getter and setter
        public String getId() {
            return this.id;
        }

        public String getNeighbors() {
            return this.neighbors;
        }

        public int getDistance() {
            return this.distance;
        }

        public String getState() {
            return this.state;
        }
    }

    /**
     * Mapper of the assignment
     *
     * Read the current state of the graph and process the Node: <br>
     * 1. if state of Node is Done, then print it out without any other operation<br>
     * 2. if state of Node is Currently Processing, then change its state into Done, and print it out<br>
     * The process of its neighbors is similar, only increase the distance by 1. Do not change the neighbors
     * field<br>
     * 3. if state of Node is Pending, then change its state into Currently Processing and print it out
     * the mapper create a Node object and receive the input data, and
     * based on the current condition of the Node<br>
     *
     * The demo of Mapper:<br>
     *  id  neighbors  distance  state          Mapper
     *  1   2,3,4      0          C        >>>>>>>>>>>>>>>   1  2,3,4  0  D
     *                                                       2  ""     1  C
     *                                                       3  ""     1  C
     *                                                       4  ""     1  C
     *
     *  2   1,4                                              2  1,4    -1 P
     *  3   1,5,6                                            3  1,5,6  -1 P
     *  4   1,2                                              ...
     *  5   3,6
     *  6   3,5
     *  7   6
     *
     */
    public static class GraphPathMapper extends Mapper<Object, Text, Text, Text> {
        // the key of mapper is default key set by MapReduce, maybe the line number
        // the value is the record of each line: id + neighbors + distance + state
        public void map(Object key, Text value, Context context) {
            try {
                Node n = new Node(value);

                if (n.getState().equals("C")) { // if current state is at C state
                    context.write(new Text(n.getId()), new Text(n.getNeighbors() + "\t" +
                            n.getDistance() + "\t" + "D")); // change its state to D and output it

                    // Output each neighbor of current node
                    // Increment the distance of each neighbor by 1
                    for (String neighbor : n.getNeighbors().split(",")) {
                        context.write(new Text(neighbor), new Text("\t" + (n.getDistance() + 1) + "\tC"));
                    }

                } else { // else output pending node without any change
                    context.write(new Text(n.getId()), new Text(n.getNeighbors() + "\t" + n.getDistance()
                            + "\t" + n.getState()));
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * The demo of Reducer:
     *                For the output form of Mapper is like:
     *                                  key    value                  Reducer
     *                    for Node 1:   1      2,3,4  0  D          >>>>>>>>>>>>>           1  2,3,4  0  D
     *
     *                    for Node 2:   2      ""     1  C
     *                                                              >>>>>>>>>>>>>           2  1,4    1  C
     *                                  2      1,4    -1 P
     *
     */
    public static class GraphPathReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                // initialize fields of final output
                String neighbors = null;
                int distance = -1;
                String state = "P";

                for (Text t : values) {
                    Node n = new Node(key, t);
                    // A node should be a final output if it is with state Done
                    // ignore remaining values
                    if (n.getState().equals("D")) {
                        neighbors = n.getNeighbors();
                        distance = n.getDistance();
                        state = n.getState();
                        break;
                    }
                    // else if current node's state is not Done
                    if (n.getNeighbors() != null && !n.getNeighbors().equals("")) {
                        neighbors = n.getNeighbors();
                    }
                    if (n.getDistance() > distance) {  // filter out initial -1
                        distance = n.getDistance();
                    }
                    if (n.getState().equals("D") || (n.getState().equals("C") && state.equals("P"))) {
                        state = n.getState();
                    }
                }
                context.write(key, new Text(neighbors + "\t" + distance + "\t" + state));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, GraphPath.class.getName());
            job.setJarByClass(GraphPath.class);

            job.setMapperClass(GraphPathMapper.class);
            job.setReducerClass(GraphPathReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // input initially is graph.txt
            // after first time running program, the input of next time operation is
            // the output of last time
//            String inputPath1 = "src/graph/graph.txt";
//            String inputPath2 = "src/graph/output1/part-r-00000";
//            String inputPath3 = "src/graph/output2/part-r-00000";
            String inputPath4 = "src/graph/output3/part-r-00000";
            String outputPath = "src/graph/output4";
            FileInputFormat.setInputPaths(job, new Path(inputPath4));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch(IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
