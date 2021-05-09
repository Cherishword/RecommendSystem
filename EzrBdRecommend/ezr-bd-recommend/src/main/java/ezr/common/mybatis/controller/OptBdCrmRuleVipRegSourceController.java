package ezr.common.mybatis.controller;

import ezr.bigdata.db.crm.CrmRuleVipRegSource;
import ezr.common.mybatis.bean.OptBdCrmRuleVipRegSourceBean;

import java.util.*;

/**
 * crm_rule_vip_reg_source 表操作
 */
public class OptBdCrmRuleVipRegSourceController {

    /**
     * 获取 crm_rule_vip_reg_source 表
     * grpType,platFormId,BrandId,Id 字段
     *
     * SELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in(-100000,1,2,20)
     *  and GrpType='TPS'
     *  ORDER By brandId,id
     *  比如：
     *      TPS	103	1	1551
     *      TPS	120	1	1552
     *      TPS	107	1	1553
     *      TPS	104	1	1554
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdCrmRuleVipRegSourceBean> getVipRegSource(List<Integer> brands, String dataCenter) {
        return CrmRuleVipRegSource.getVipRegSource(brands,dataCenter);
    }

    /**
     * 获取 crm_rule_vip_reg_source 表
     * grpType,platFormId,BrandId,Id 字段
     *
     * SELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in(-100000,1,2,20)
     *  and GrpType='TPS'
     *  ORDER By brandId,id
     *  比如：
     *      TPS	103	1	1551
     *      TPS	120	1	1552
     *      TPS	107	1	1553
     *      TPS	104	1	1554
     *   最后把结果封装到map里
     *   map<brandid_id,platFormId>
     *   比如：
     *      1_1552	1
     *      1_1551	0
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String,Integer> getPlatformId(List<Integer> brands, String dataCenter){
        return CrmRuleVipRegSource.getPlatformId(brands,dataCenter);
    }

    /**
     * 获取crm_rule_vip_reg_source表的grpType,platFormId,BrandId,Id字段
     * 切根据条件id > 1000 和指定的品牌
     * ELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in (-100000,1,2,20)
     *  and id > 1000
     *  ORDER By id
     *
     *  比如:
     *      SMCP	0	1	1001
     *      GAME	0	1	1002
     *      SMCP	0	1	1003
     *      CQRC	0	1	1004
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdCrmRuleVipRegSourceBean> getVipGrpType(List<Integer> brands, String dataCenter) {
        return CrmRuleVipRegSource.getVipGrpType(brands,dataCenter);
    }


    /**
     * 获取crm_rule_vip_reg_source表的grpType,platFormId,BrandId,Id字段
     * 切根据条件id > 1000 和指定的品牌
     * ELECT grpType,platFormId,BrandId,Id FROM crm_rule_vip_reg_source
     *  where BrandId in (-100000,1,2,20)
     *  and id > 1000
     *  ORDER By id
     *
     *  比如:
     *      SMCP	0	1	1001
     *      GAME	0	1	1002
     *      SMCP	0	1	1003
     *      CQRC	0	1	1004
     *  然后根据grpType 赋值 1-8
     *  结果放到map里
     *  map<brandid_id,1~8>
     *  比如：
     *      1_2199	2
     *      1_2198	2
     *      1_2197	2
     *      1_2196	3
     *      1_2195	2
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String,Integer> getGrpType(List<Integer> brands, String dataCenter){
        return CrmRuleVipRegSource.getGrpType(brands,dataCenter);
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);

        List<OptBdCrmRuleVipRegSourceBean>  lst1 = OptBdCrmRuleVipRegSourceController.getVipRegSource(inList,"test");
        for(OptBdCrmRuleVipRegSourceBean c:lst1){
            //grpType,platFormId,BrandId,Id
            System.out.println(c.getGrpType()+"\t"+c.getPlatFormId()+"\t"+c.getBrandId()+"\t"+c.getId());
        }

        Map<String,Integer> m = OptBdCrmRuleVipRegSourceController.getPlatformId(inList,"test");
        for(Map.Entry<String,Integer> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }

        List<OptBdCrmRuleVipRegSourceBean>  lst2 = OptBdCrmRuleVipRegSourceController.getVipGrpType(inList,"test");
        for(OptBdCrmRuleVipRegSourceBean c:lst2){
            //grpType,platFormId,BrandId,Id
            System.out.println(c.getGrpType()+"\t"+c.getPlatFormId()+"\t"+c.getBrandId()+"\t"+c.getId());
        }

        Map<String,Integer> m2 = OptBdCrmRuleVipRegSourceController.getGrpType(inList,"test");
        for(Map.Entry<String,Integer> e:m2.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue());
        }
    }
}
