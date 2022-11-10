package com.ju4t.mapreduce.outputFormat;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 输入 http://www.baidu.com

        // 输出 (http://www.baidu.com, NullWritable)
        // 在 map 阶段不做任何处理
        context.write(value, NullWritable.get());
    }
}
