package com.ju4t.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 处理 pd.txt
        // 01	小米
        // 02	华为
        // 03	格力
        // 1.获取缓存文件，并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();

        // 2.循环读取缓存文件一行
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        // 从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;

        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 3.切割
            // 4.缓存数据到集合
            String[] fields = line.split("\t");
            // 赋值
            pdMap.put(fields[0], fields[1]);
        }


        // 5.关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 处理 order.txt
        // 1.获取一行
        // 2.截取
        // 3.获取pid
        // 4.获取订单id和商品名称
        // 5.拼接
        // 6.写出

        // 处理 order.txt
        String line = value.toString();
        String[] fileds = line.split("\t");

        // 获取pid
        String pname = pdMap.get(fileds[1]);

        // 获取订单id 和 订单数量
        outK.set(fileds[0] + "\t" + pname + "\t" + fileds[2]);

        context.write(outK, NullWritable.get());
    }
}
