package com.aceforeverd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class App
{
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        public void map(LongWritable key, Text value, Context  context) throws IOException, InterruptedException {
            kv = value.toString().split(",");

            if (deptMap.containsKey(kv[3])) {
                if (kv[2] != null && ! "".equals(kv[2].toString())) {
                    context.write(new Text(deptMap.get(kv[3].trim())), new Text(kv[2].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sumSalary = 0;
            for (Text val : values) {
                sumSalary += Long.parseLong(val.toString());
            }
            context.write(key, new LongWritable(sumSalary));
        }
    }

    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "dept emp");
        job.setJarByClass(App.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}
