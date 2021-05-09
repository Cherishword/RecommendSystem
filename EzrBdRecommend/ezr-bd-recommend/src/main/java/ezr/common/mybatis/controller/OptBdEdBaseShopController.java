package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdBaseShop;
import ezr.common.mybatis.bean.OptBdEdBaseShopBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OptBdEdBaseShopController {

    /**
     * 根据品牌获取门店信息
     * select Id,BrandId,Name,Code from ed_base_shop
     *  where BrandId in (-100000,1,2,20)
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static List<OptBdEdBaseShopBean> getByName(List<Integer> brands, String dataCenter) {
        return EdBaseShop.getByName(brands,dataCenter);
    }

    /**
     * 根据品牌获取门店名字
     * select Id,Name from ed_base_shop
     *  where BrandId in (-100000,1,2,20)
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String, String> getShopName(List<Integer> brands,String dataCenter){

        return EdBaseShop.getShopName(brands,dataCenter);
    }

    /**
     * 根据品牌获取门店Code
     * select Id,Code from ed_base_shop
     *  where BrandId in (-100000,1,2,20)
     *
     * @param brands
     * @param dataCenter
     * @return
     */
    public static Map<String, Integer> getShopCode(List<Integer> brands,String dataCenter){

        return EdBaseShop.getShopCode(brands,dataCenter);
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdEdBaseShopBean> lst = OptBdEdBaseShopController.getByName(inList,"test");
        for(OptBdEdBaseShopBean ob:lst){
            System.out.println(ob.getId()+"\n"+ob.getBrandId()+"\t"+ob.getCode()+"\t"+ob.getName());
        }

        Map<String, Integer>  map = OptBdEdBaseShopController.getShopCode(inList,"test");
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }

        Map<String, Integer>  map2 = OptBdEdBaseShopController.getShopCode(inList,"test");
        for(Map.Entry<String, Integer> entry : map2.entrySet()){
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }

    }

}
