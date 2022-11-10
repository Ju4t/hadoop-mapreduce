package com.ju4t.mapreduce.partitioner;


import com.ju4t.mapreduce.partitioner2.ProvincePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置 jar 路径
        job.setJarByClass(WordCountDriver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 设置 mapper 的输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置 最终输出的 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输出文件
        job.setNumReduceTasks(2);

        // 6. 设置 输入路径、输出路径
        FileInputFormat.setInputPaths(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/data/wordcount"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/output/partitioner"));


        // 7. 提交 job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
