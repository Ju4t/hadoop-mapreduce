package com.ju4t.mapreduce.partitionerAndwiteableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 获取job对象
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        // 2. 设置jar包
        job.setJarByClass(FlowDriver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4. 设置 mappper 的 key 和 value 类型
        // FlowBean，Text 更换
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 设置 最终数据输出 的 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置输出文件
        job.setPartitionerClass(ProvincePartitioner2.class);
        job.setNumReduceTasks(5);

        // 6. 设置 数据的输入、输出路径
        // 输出文件用上一次的分组求和的结果
        FileInputFormat.setInputPaths(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/output/phoneout"));
        FileOutputFormat.setOutputPath(job, new Path("/Volumes/Data/Dev/Java/hadoop-mapreduce/output/partitionerandwriteablecomparable"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交 job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
