package com.hh.demohadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountHadoop {

    private static Jedis jedis = new Jedis("localhost", 6379);

    public static void main(String [] args) throws Exception {

        Configuration conf = new Configuration();

        Job j = new Job(conf,"wordcount");
        j.setJarByClass(WordCountHadoop.class);
        j.setMapperClass(MapForWordCount.class);
        j.setCombinerClass(ReduceForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, new Path("hdfs://localhost:9000/user/abc/demohadoop.txt"));
//        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path("hdfs://localhost:9000/user/abc/output1.txt"));
//        FileOutputFormat.setOutputPath(j, new Path(args[1]));
        System.exit(j.waitForCompletion(true)?0:1);
    }

    public static class MapForWordCount extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            jedis.connect();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            jedis.set(key.toString(), String.valueOf(sum));
            result.set(sum);
            context.write(key, result);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            jedis.disconnect();
        }
    }
}
