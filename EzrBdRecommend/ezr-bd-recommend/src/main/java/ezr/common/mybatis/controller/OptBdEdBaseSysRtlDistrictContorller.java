package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdBaseSysRtlDistrict;
import ezr.common.mybatis.bean.OptBdEdBaseSysRtlDistrictBean;
import ezr.common.mybatis.bean.RtlDistrictNameAndIdBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 片区和门店
 */
public class OptBdEdBaseSysRtlDistrictContorller {

    /**获取门店id,片区名称，品牌，片区id
     *SELECT a.ShopId ShopId,b.NAME name,b.BrandId BrandId,b.id id
     *      FROM ed_base_sys_rtl_district_dtl a
     *      LEFT JOIN ed_base_sys_rtl_district b
     *      on b.id=a.RtlDistrictId
     *      where b.BrandId in (-100000,1,2,20)
     * @param brands
     * @return
     */
    public static List<OptBdEdBaseSysRtlDistrictBean> getByRtlName(List<Integer> brands) {
        return EdBaseSysRtlDistrict.getByRtlName(brands);
    }

    /**
     * 把得到的门店id,片区名称，品牌，片区id
     * 封装到map里
     *  map<品牌id_shopId,(片区id，片区名称)>
     * @param brands
     * @return
     */
    public static Map<String, RtlDistrictNameAndIdBean> getByRtlDistrictName(List<Integer> brands){
        return EdBaseSysRtlDistrict.getByRtlDistrictName(brands);
    }

    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdEdBaseSysRtlDistrictBean>  srd =  OptBdEdBaseSysRtlDistrictContorller.getByRtlName(inList);
        for(OptBdEdBaseSysRtlDistrictBean s:srd){
            System.out.println(s.getId()+"\t"+s.getShopId()+"\t"+s.getName()+"\t"+s.getBrandId());
        }

        Map<String, RtlDistrictNameAndIdBean> m = OptBdEdBaseSysRtlDistrictContorller.getByRtlDistrictName(inList);
        for(Map.Entry<String, RtlDistrictNameAndIdBean> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue().getRtlDistrictId()+"\t"+e.getValue().getName());
        }
    }
}
