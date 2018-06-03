import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class IOT2 {
    private static String DEVICE = "device";
    private static String DVALUE = "dvalue";

    public static class ReduceOutKey {
        public String type;
        public String date;

        public ReduceOutKey(String t, String d) {
            type = t;
            date = d;
        }

        public String toString() {
            return type + "\t" + date;
        }
    }

    public static class OutKeyComp implements java.util.Comparator<ReduceOutKey> {
        public int compare(ReduceOutKey left, ReduceOutKey right) {
            int r = left.date.compareTo(right.date);
            if (r < 0) {
                return 1;
            } else if (r > 0) {
                return -1;
            }

            return left.type.compareTo(right.type);
        }
    }

    public static class ReduceOutVal {
        public double sum;
        public int count;

        public ReduceOutVal() {
            sum = 0.0;
            count = 0;
        }

        public void add(double val) {
            sum += val;
            count ++;
        }

        public double avg() {
            if (count <= 0) {
                return 0.0;
            }

            return sum / count;
        }

        public String toString() {
            return Double.toString(sum / count);
        }
    }

    public static class DeviceMapper extends Mapper<Object, Text, IntWritable, Text> {
        private String[] fields;
        private int id;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            fields = value.toString().split("\\s+");
            if (fields.length < 3) {
                return;
            }

            try {
                id = Integer.parseInt(fields[0].trim());
            } catch (Exception e) {
                return;
            }

            /* id + device + type */
            context.write(new IntWritable(id), new Text(DEVICE + " " + fields[1].trim()));
        }
    }

    public static class DvalueMapper extends Mapper<Object, Text, IntWritable, Text> {
        private String[] fields;
        private int did;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            fields = value.toString().split("\\s+");
            if (fields.length < 3) {
                return;
            }

            did = Integer.parseInt(fields[0].trim());
            if (did > 0 && did < 10 && !fields[1].trim().equals("\\N")) {
                /* did + dvalue + date + value */
                context.write(new IntWritable(did), new Text(DVALUE + " " + fields[1] + " " + fields[2]));
            }
        }
    }

    public static class LeftJoinReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        private String[] fields;
        private TreeMap<ReduceOutKey, ReduceOutVal> maps = new TreeMap<>(new OutKeyComp());

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String type = "";
            String date = "";
            double value = 0.0;
            ReduceOutKey outKey;
            ReduceOutVal outVal;
            for (Text val : values) {
                fields = val.toString().split("\\s+");
                if (fields.length < 3) {
                    return;
                }

                if (fields[0].trim().equals(DEVICE)) {
                    type = fields[1].trim();
                } else if (fields[0].trim().equals(DVALUE)) {
                    date = fields[1].trim();
                    try {
                        value = Double.parseDouble(fields[2].trim());
                    } catch (Exception e) {
                        continue;
                    }
                } else {
                    continue;
                }

                outKey = new ReduceOutKey(type, date);
                if (maps.containsKey(outKey)) {
                    outVal = maps.get(outKey);
                    outVal.add(value);
                    maps.replace(outKey, outVal);
                } else {
                    outVal = new ReduceOutVal();
                    outVal.add(value);
                    maps.put(outKey, outVal);
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<ReduceOutKey, ReduceOutVal> entry : maps.entrySet()) {
                context.write(new Text(entry.getKey().date + " " + entry.getKey().type),
                        new DoubleWritable(entry.getValue().avg()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IOT2");
        job.setJarByClass(IOT2.class);

        job.setReducerClass(LeftJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        if (args.length < 3) {
            System.err.println("requre Device input, Dvalue input and output path");
            System.exit(1);
        }
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DeviceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DvalueMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
