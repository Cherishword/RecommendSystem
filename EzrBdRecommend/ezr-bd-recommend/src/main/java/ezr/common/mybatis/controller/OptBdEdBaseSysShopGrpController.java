package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdBaseSysShopGrp;
import ezr.common.mybatis.bean.OptBdEdBaseSysShopGrpBean;
import ezr.common.mybatis.bean.ShopGrpNameAndIdBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 店群和门店
 */
public class OptBdEdBaseSysShopGrpController {
    /** 获取门店id,店群名称，品牌id，店群id
     * SELECT a.ShopId ShopId,b.NAME name,b.BrandId BrandId,a.ShopGrpId ShopGrpId
     *      FROM ed_base_sys_shop_grp_dtl a
     *      LEFT JOIN ed_base_sys_shop_grp b
     *      on b.id=a.ShopGrpId
     *      where b.BrandId in(-100000,1,2,20)
     * @param brands
     * @return
     */
    public static List<OptBdEdBaseSysShopGrpBean> getByShopName(List<Integer> brands) {
        return EdBaseSysShopGrp.getByShopName(brands);
    }

    /**
     * 获取门店id,店群名称，品牌id，店群id
     * 封装到map里
     * map<branid_shopId,(店群id,店群名称)>
     * @param brands
     * @return
     */
    public static Map<String, ShopGrpNameAndIdBean> getByShopGrpName(List<Integer> brands){
        return EdBaseSysShopGrp.getByShopGrpName(brands);
    }

    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdEdBaseSysShopGrpBean> lst = OptBdEdBaseSysShopGrpController.getByShopName(inList);
        for(OptBdEdBaseSysShopGrpBean ssg:lst){
            System.out.println(ssg.getBrandId()+"\t"+ssg.getShopGrpId()+"\t"+ssg.getName()+"\t"+ssg.getShopId());
        }

        Map<String, ShopGrpNameAndIdBean> m = OptBdEdBaseSysShopGrpController.getByShopGrpName(inList);
        for(Map.Entry<String, ShopGrpNameAndIdBean> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue().getShopGrpId()+e.getValue().getName());
        }
    }
}
