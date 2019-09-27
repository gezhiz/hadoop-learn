package com.worthto.niuniu.countflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class FlowCountReducer extends Reducer<Text,FlowBeanWritable,Text,FlowBeanWritable> {

    @Override
    protected void reduce(Text key, Iterable<FlowBeanWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<FlowBeanWritable> iterator = values.iterator();
        String phone = key.toString();
        Long upCount = 0L;
        Long dCount = 0L;
        Long amount = 0L;
        if (iterator.hasNext()) {
            FlowBeanWritable flowBeanWritable = iterator.next();
            upCount += flowBeanWritable.getUpFlow();
            dCount += flowBeanWritable.getdFlow();
            amount += flowBeanWritable.getAmountFlow();
        }
        FlowBeanWritable flowBeanWritable = new FlowBeanWritable(upCount,dCount,amount,phone);
        context.write(key,flowBeanWritable);
    }
}
