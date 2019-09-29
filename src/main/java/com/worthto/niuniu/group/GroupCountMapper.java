package com.worthto.niuniu.group;

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
public class GroupCountMapper extends Mapper<LongWritable,Text,Text,GroupBeanWritable> {

    private static int LINE_LEN = 5;
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
        if (items == null || items.length != LINE_LEN) {
            return;
        }
        String phone = items[0];
//        String site = items[1];
        Long upFlow = Long.parseLong(items[2]);
        Long dFlow = Long.parseLong(items[3]);
        String province = items[4];
        try {
            context.write(new Text(phone), new GroupBeanWritable(upFlow, dFlow, upFlow + dFlow, phone, province));
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }
}
