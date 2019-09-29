package com.worthto.niuniu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 接口访问统计Mapper，中间结果交给另外一个MapReduce处理
 * @author gezz
 * @description todo
 * @date 2019/9/26.
 */
public class SortStep1Mapper extends Mapper<LongWritable,Text,Text,LongWritable> {

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
}
