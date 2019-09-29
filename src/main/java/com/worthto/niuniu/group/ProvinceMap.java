package com.worthto.niuniu.group;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/29.
 */
public class ProvinceMap {
    private static Map<String, Integer> provinces = new HashMap();
    private static int OTHERS_PROVINCE = 21;
    private final static int size;

    public static Integer getPartition(String province) {
        Integer result = provinces.get(province);
        return result == null ? OTHERS_PROVINCE : result;
    }


    static {
        provinces.put("江西省", 0);
        provinces.put("北京市", 1);
        provinces.put("天津市", 2);
        provinces.put("山东省", 3);
        provinces.put("河南省", 4);
        provinces.put("广东省", 5);
        provinces.put("广西省", 6);
        provinces.put("河北省", 7);
        provinces.put("安徽省", 8);
        provinces.put("重庆市", 9);
        provinces.put("四川省", 10);
        provinces.put("浙江省", 11);
        provinces.put("江苏省", 12);
        provinces.put("上海市", 13);
        provinces.put("海南省", 14);
        provinces.put("台湾省", 15);
        provinces.put("黑龙江省", 16);
        provinces.put("辽宁省", 17);
        provinces.put("吉林省", 18);
        provinces.put("湖南省", 19);
        provinces.put("河北省", 20);
        provinces.put("其他省", 21);
        size = provinces.size() + 1;
    }

    public static int getProvinceNums() {
        return size;
    }
}
