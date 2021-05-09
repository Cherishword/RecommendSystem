package ezr.common.mybatis.controller;

import ezr.bigdata.db.crm.CrmRuleBonusSell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liucf on 2018/5/14.
 */
public class OptCrmRuleBonusSellController {
    /**
     * 获取对应品牌的积分过期预警时间
     * @param brands 品牌
     * @return 返回依品牌为key,过期预警时间为value的map
     */
    public static Map<Integer,Integer> getExpireWarnDays(List<Integer> brands,String dataCenter){
        return CrmRuleBonusSell.getExpireWarnDays(brands,dataCenter);
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        Map<Integer,Integer> m = OptCrmRuleBonusSellController.getExpireWarnDays(inList,"test");
        for(Map.Entry<Integer,Integer> e : m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }
    }
}
