package com.worthto.niuniu.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/29.
 */
public class ProvincePartitioner extends Partitioner<Text,GroupBeanWritable> {
    @Override
    public int getPartition(Text key, GroupBeanWritable value, int numPartitions) {
        return ProvinceMap.getPartition(value.getProvince());
    }
}
