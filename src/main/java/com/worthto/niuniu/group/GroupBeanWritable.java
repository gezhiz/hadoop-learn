package com.worthto.niuniu.group;

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
public class GroupBeanWritable implements Writable {
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

    private String province;

    public GroupBeanWritable() {}

    public GroupBeanWritable(long upFlow, long dFlow, long amountFlow, String phone, String province) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.amountFlow = amountFlow;
        this.phone = phone;
        this.province = province;
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

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(amountFlow);
        //Writes two bytes of length information
        out.writeUTF(phone);
        out.writeUTF(province);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.dFlow = in.readLong();
        this.amountFlow = in.readLong();
        //Reads two bytes of length information
        this.phone = in.readUTF();
        this.province = in.readUTF();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dFlow + "\t" + amountFlow + "\t" + phone + "\t" + province + '\t';
    }
}
