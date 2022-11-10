package com.ju4t.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 1. 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置加载jar包路径
        job.setJarByClass(MapJoinDriver.class);

        // 3. 关联mapper
        job.setMapperClass(MapJionMapper.class);

        // 4. 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5. 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6. 加载缓存数据
        job.addCacheFile(new URI("/Volumes/Data/Dev/Java/hadoop-mapreduce/data/table/pd.txt"));
        job.setNumReduceTasks(0);

        // 7. 设置最终输入输出路径（不包含pd.txt）
        FileInputFormat.setInputPaths(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/data/table2"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/output/table2"));

        // 8. 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
