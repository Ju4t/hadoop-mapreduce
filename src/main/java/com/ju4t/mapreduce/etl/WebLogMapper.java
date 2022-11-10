package com.ju4t.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();

        // 2. 切割
        String[] split = line.split(" ");

        // 3. ETL
        boolean result = parseLog(line, context);

        if (!result) {
            return;
        }

        // 4.写出
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        // 解析Log规则
        // 切割
        String[] fields = line.split(" ");

        if (fields.length > 11) {
            return true;
        } else {
            return false;
        }

    }
}
