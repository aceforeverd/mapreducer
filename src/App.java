import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class App
{
    public static class MapClass extends Mapper<Object, Text, Text, LongWritable> {
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path path = new Path("hdfs:/data/input/dept.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            String[] columns;
            while (true) {
                line = br.readLine();
                if (line == null) {
                    break;
                }
                columns = line.split(",");
                if (columns.length < 3) {
                    continue;
                }
                if (!deptMap.containsKey(columns[0].trim())) {
                    deptMap.put(columns[0].trim(), columns[1].trim());
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            kv = value.toString().split(",");

            /* emp */
            if (deptMap.containsKey(kv[3].trim())) {
                if (kv[2] != null && ! "".equals(kv[2].toString())) {
                    context.write(new Text(deptMap.get(kv[3].trim())),
                            new LongWritable(Long.parseLong(kv[2].trim())));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sumSalary = 0;
            for (LongWritable val : values) {
                sumSalary += val.get();
            }
            context.write(key, new LongWritable(sumSalary));
        }
    }

    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "emp");
        job.setJarByClass(App.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}
