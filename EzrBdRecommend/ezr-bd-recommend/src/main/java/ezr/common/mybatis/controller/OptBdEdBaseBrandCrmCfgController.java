package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdBaseBrandCrmCfg;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liucf on 2018/8/2.
 */
public class OptBdEdBaseBrandCrmCfgController {
    /**
     * vipSalStaticsType=1：按表头统计
     * vipSalStaticsType=2：按明细统计
     *select BrandId from ed_base_brand_crm_cfg
     *      where vipSalStaticsType = 1
     *      and brandId in(-100000,1,2,20)
     * 拿到按表头或者按明细统计的品牌的Id
     * @param brandIds
     * @param vipSalStaticsType
     * @return
     */
    public static List<Integer> getBrandIds(List<Integer> brandIds,int vipSalStaticsType){
        return EdBaseBrandCrmCfg.getBrandIds(brandIds,vipSalStaticsType);
    }


    /**
     * 获取开通了非会员订单的品牌id
     *select brandId from ed_base_brand_crm_cfg
     *  where IsNoVipOrder = 1
     *  and brandId in(-100000,1,2,20)
     * @param brandIds
     * @param isNoVipOrder
     * @param dataCenter
     * @return
     */
    public static List<Integer> getBrandIdsByIsNoVipOrder(List<Integer> brandIds,String isNoVipOrder, String dataCenter){
        return EdBaseBrandCrmCfg.getBrandIdsByIsNoVipOrder(brandIds,isNoVipOrder,dataCenter);
    }

    public static void main(String[] args){
        /**1*/
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<Integer>  lst1 = OptBdEdBaseBrandCrmCfgController.getBrandIds(inList,1);
        for(int i:lst1){
            System.out.println(i);
        }

        List<Integer>  lst2 = OptBdEdBaseBrandCrmCfgController.getBrandIdsByIsNoVipOrder(inList,"1","test");
        for(int i:lst2){
            System.out.println(i);
        }
    }
}
