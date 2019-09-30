package com.worthto.niuniu.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class GroupCountReducer extends Reducer<Text, GroupBeanWritable, Text, GroupBeanWritable> {

    @Override
    protected void reduce(Text key, Iterable<GroupBeanWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<GroupBeanWritable> iterator = values.iterator();
        String phone = key.toString();
        Long upCount = 0L;
        Long dCount = 0L;
        Long amount = 0L;
        String province = "";
        boolean first = true;
        while (iterator.hasNext()) {
            GroupBeanWritable groupBeanWritable = iterator.next();
            upCount += groupBeanWritable.getUpFlow();
            dCount += groupBeanWritable.getdFlow();
            amount += groupBeanWritable.getAmountFlow();
            if (first) {
                first = false;
                province = groupBeanWritable.getProvince();
            }
        }
        GroupBeanWritable groupBeanWritable = new GroupBeanWritable(upCount, dCount, amount, phone, province);
        context.write(key, groupBeanWritable);
    }
}
