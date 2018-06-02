import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class IOT1 {
    public static class DeviceMapper extends Mapper<Object, Text, IntWritable, Text> {
        private String[] fields;
        private int id;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            fields = value.toString().split("\\s+");
            if (fields.length < 3) {
                return;
            }
            id = Integer.parseInt(fields[0].trim());
            if (id <= 0 || id >= 1000) {
                return;
            }

            /* id + device + type */
            context.write(new IntWritable(id), new Text("device " + fields[1].trim()));
        }
    }

    public static class DValueMapper extends Mapper<Object, Text, IntWritable, Text> {
        private String[] fields;
        private int did;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            fields = value.toString().split("\\s+");

            if (fields.length < 3) {
                return;
            }

            did = Integer.parseInt(fields[0].trim());
            if (did <= 0 || did >= 1000) {
                return;
            }

            if (!fields[1].trim().equals("\\N")) {
                return;
            }

            /* did - value - date */
            context.write(new IntWritable(did), new Text("value " + fields[2].trim()));
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        private TreeMap<String, Double> maps = new TreeMap<>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String type = "unknown";
            double sumValue = 0.0;
            String[] fields;

            for (Text val : values) {
                fields = val.toString().split("\\s+");
                if (fields.length < 2) {
                    /* not valid, ignore */
                    continue;
                }

                if (fields[0].trim().equals("device")) {
                    type = fields[1].trim();
                } else if (fields[0].trim().equals("value")) {
                    double value;
                    try {
                        value = Double.parseDouble(fields[1].trim());
                        sumValue += value;
                    } catch (Exception e) {
                        continue;
                    }
                }
            }

            maps.put(type, sumValue);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : maps.descendingMap().entrySet()) {
                context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IOT1");
        job.setJarByClass(IOT1.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DeviceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DValueMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
