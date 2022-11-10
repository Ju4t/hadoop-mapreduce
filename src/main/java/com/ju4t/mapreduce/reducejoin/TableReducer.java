package com.ju4t.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 01 1001 1 order
        // 02 1004 4 order

        // 01 小米    pd

        // 准备初始化集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        // 循环遍历
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) { // 订单表
                // orderBeans.add(value); 不能用这种方法，hadoop和java有区别的地方
                // 如下处理：
                TableBean tmpTableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpTableBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmpTableBean);
            } else { // 商品表
                // 进来的数据只能是一行
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //循环遍历orderBeans，赋值pdname
        for (TableBean orderBean : orderBeans) {
           orderBean.setPname(pdBean.getPname());
           context.write(orderBean, NullWritable.get());
        }

    }
}
