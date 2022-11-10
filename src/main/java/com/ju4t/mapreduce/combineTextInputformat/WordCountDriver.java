package com.ju4t.mapreduce.combineTextInputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置4m=4*1024*1024
        // 默认 一个文件切一片 切4片， 4M 切成3片，20M 切成1片
        // CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 6. 设置 输入路径、输出路径
        // 小文件路径
        FileInputFormat.setInputPaths(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/data/combine"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/output/combine"));

        // 7. 提交 job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
