package com.ju4t.mapreduce.yasuo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 获取job
        Configuration conf = new Configuration();

        // ** 开启map端压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        // SnappyCodec 压缩方式：是需要hadoop3.x 和 Centos7以上的版本配合才能实现
        // conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);

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

        // 6. 设置 输入路径、输出路径
        FileInputFormat.setInputPaths(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/data/wordcount"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/output/yasuo"));

        // ** 开启 reduce 端压缩
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        // FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        // 提交 job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
