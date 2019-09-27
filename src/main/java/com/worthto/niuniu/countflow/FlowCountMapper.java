package com.worthto.niuniu.countflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/26.
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBeanWritable> {

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
        if (value == null) {
            return;
        }
        String line = value.toString();
        String[] items = line.split("\t");
        if (items == null || items.length != 4) {
            return;
        }
        String phone = items[0];
//        String site = items[1];
        Long upFlow = Long.parseLong(items[2]);
        Long dFlow = Long.parseLong(items[3]);
        context.write(new Text(phone), new FlowBeanWritable(upFlow, dFlow, upFlow + dFlow, phone));
    }
}
