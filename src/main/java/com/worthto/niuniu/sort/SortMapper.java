package com.worthto.niuniu.sort;

import org.apache.hadoop.io.IntWritable;
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
public class SortMapper extends Mapper<LongWritable,Text,PageCount,IntWritable> {

    private static IntWritable ONE = new IntWritable(1);
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
        String[] items = line.split("\t");
        context.write(new PageCount(items[0], Long.parseLong(items[1])), ONE);
    }
}
