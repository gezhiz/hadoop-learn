package com.worthto.niuniu.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计流量：自定义序列化
 * @author gezz
 * @description todo
 * @date 2019/9/26.
 */
public class TopnMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    /**
     * key 表示偏移量
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(" ");
        String curItem = items[1];
        context.write(new Text(curItem),new LongWritable(1));
    }

    /**
     * map任务执行完之前执行
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // NOTHING
    }
}
