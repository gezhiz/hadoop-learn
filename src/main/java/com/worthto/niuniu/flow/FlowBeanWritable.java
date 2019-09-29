package com.worthto.niuniu.flow;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 如何实现Hadoop的序列化接口
 * 注意：
 * 1、需要无参构造
 * 2、读取数据的顺序要和解析数据一致
 * @author gezz
 * @description todo
 * @date 2019/9/26.
 */
public class FlowBeanWritable implements Writable {
    /**
     * 上行流量，上传流量
     */
    private long upFlow;
    /**
     * 下行流量，下载流量
     */
    private long dFlow;
    /**
     * 总流量
     */
    private long amountFlow;
    /**
     * 用户的电话
     */
    private String phone;

    public FlowBeanWritable() {}

    public FlowBeanWritable(long upFlow, long dFlow, long amountFlow, String phone) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.amountFlow = amountFlow;
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public long getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(long amountFlow) {
        this.amountFlow = amountFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(amountFlow);
        //Writes two bytes of length information
        out.writeUTF(phone);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.dFlow = in.readLong();
        this.amountFlow = in.readLong();
        //Reads two bytes of length information
        this.phone = in.readUTF();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
