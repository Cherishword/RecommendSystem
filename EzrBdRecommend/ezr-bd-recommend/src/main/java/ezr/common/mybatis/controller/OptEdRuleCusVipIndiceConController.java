package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdRuleCusVipIndiceCon;
import ezr.common.mybatis.bean.OptEdRuleCusVipInfoConBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liucf on 2018/5/15.
 * 针对 mysql opt 库下oopt_bd_ed_rule_cus_vip_indice_con表的操作
 */
public class OptEdRuleCusVipIndiceConController {


    /**
     * 根据传进来的brandid的列表 取出指定字段的值封装到EdRuleCusVipInfoConBean对象里
     * @param indiceId 要取出的字段值
     * @param brands brandid的列表
     * @return 返回 EdRuleCusVipInfoConBean对象的列表
     */
    public static List<OptEdRuleCusVipInfoConBean> getByIndiceId(String indiceId, List<Integer> brands,String dataCenter){
        return EdRuleCusVipIndiceCon.getByIndiceId(indiceId, brands,dataCenter);
    }

    /**
     * 根据brandid 取得每个品牌流失会员的定义
     * @param brands  List<Integer> brands
     * @return Map<Integer, Integer> map
     */
    public static Map<Integer, Integer > getLostDaysOfBrands(List<Integer> brands,String dataCenter) {
        return EdRuleCusVipIndiceCon.getLostDaysOfBrands(brands,dataCenter);
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptEdRuleCusVipInfoConBean>  c = OptEdRuleCusVipIndiceConController.getByIndiceId("26",inList,"test");

        for(OptEdRuleCusVipInfoConBean cc : c){
            System.out.println(cc.getId()+"\t"+cc.getBrandId()+"\t"+cc.getConVal());
        }

        Map<Integer, Integer > m = OptEdRuleCusVipIndiceConController.getLostDaysOfBrands(inList,"test");
        for(Map.Entry<Integer, Integer > e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }

    }
}
