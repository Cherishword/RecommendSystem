package ezr.common.mybatis.controller;

import ezr.bigdata.db.ed.EdBaseBrandDB;
import ezr.common.mybatis.bean.EdBaseBrand;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liucf on 2018/5/14.
 */
public class OptBdEdBaseBrandController {

    /**
     * 根据 CrmDbShardingId 获取 活跃的品牌编号
     *select Id FROM ed_base_brand
     *      where IsActive = 1
     *      and CrmDbShardingId in (-100000,1,100)
     * @param shardingIds
     * @return
     */
    public static List<Integer> getBrandIds(List<Integer> shardingIds){
        return EdBaseBrandDB.getBrandIds(shardingIds);
    }

    /**
     * 根据shardingId,和brandId列表 获取对应的有记录的品牌
     * select Id FROM ed_base_brand
     *      where CrmDbShardingId in(-100000,1)
     *      and Id in (-100000,1,106,100)
     * @param shardingIds sharding
     *  @param brandIds brandIds品牌列表
     * @return 返回品牌id
     */
    public static List<Integer> getBrandIds(List<Integer> shardingIds,List<Integer> brandIds){
        return EdBaseBrandDB.getBrandIds(shardingIds,brandIds);
    }
    /**
     * 获取品牌的CrmDbShardingId
     *select CrmDbShardingId FROM ed_base_brand
     *  where id = 1
     * @param brandId
     * @return
     */
    public static int getShardingId(int brandId){
        return EdBaseBrandDB.getShardingId(brandId);
    }
    /**
     * 查询所有没有流失的品牌
     * SELECT Id,Code,CrmDbShardingId,Name FROM ed_base_brand
     *  where IsActive = 1
     * @return
     */
    public static ArrayList<EdBaseBrand> getAllActiveBrand(){
        return EdBaseBrandDB.getAllActiveBrand();
    }

    /**
     * 查询所有已经流失的品牌信息
     * SELECT Id,Code,CrmDbShardingId,Name FROM ed_base_brand
     *  where IsActive = 0
     * @return
     */
    public static ArrayList<EdBaseBrand> getLapsedBrand(){
        return EdBaseBrandDB.getLapsedBrand();
    }
    /**
     * 非私有化部署
     *select Id FROM opt_bd_ed_base_brand
     *      where IsActive = 1
     *      and CrmDbShardingId in (-100000,1,100)
     *      and DataCenter = 'test'
     *  私有化部署用
     *  select Id FROM ed_base_brand
     *      where IsActive = 1
     *      and CrmDbShardingId in (-100000,1,100)
     * @param shardingIds
     * @param dataCenter
     * @return
     */
    public static List<Integer> getBrandIdsByDataCenter(List<Integer> shardingIds,String dataCenter){
        return EdBaseBrandDB.getBrandIdsByDataCenter(shardingIds,dataCenter);
    }


    public static void main(String[] args){
        /** 1 */
        System.out.println(OptBdEdBaseBrandController.getShardingId(1));

        /** 2 */
        ArrayList<EdBaseBrand> lst1   = OptBdEdBaseBrandController.getAllActiveBrand();
        for(EdBaseBrand ebb:lst1){
            System.out.println(ebb.getId()+"\t"+ebb.getCode()+"\t"+ebb.getCrmDbShardingId()+"\t"+ebb.getName());
        }

        /** 3 */
        ArrayList<EdBaseBrand> lst2   = OptBdEdBaseBrandController.getLapsedBrand();
        for(EdBaseBrand ebb:lst2){
            System.out.println(ebb.getId()+"\t"+ebb.getCode()+"\t"+ebb.getCrmDbShardingId()+"\t"+ebb.getName());
        }

        /** 4 */
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(100);
        List<Integer> lst3 = OptBdEdBaseBrandController.getBrandIdsByDataCenter(inList,"test");
        for(Integer i:lst3){
            System.out.println(i);
        }

        /**5*/
        List<Integer> inList2 = new ArrayList();
        inList2.add(1);
        inList2.add(100);
        List<Integer> lst4 = OptBdEdBaseBrandController.getBrandIds(inList2);
        for(Integer i:lst4){
            System.out.println(i);
        }


        /**6*/
        List<Integer> inList3 = new ArrayList();
        inList3.add(1);

        List<Integer> inList4 = new ArrayList();
        inList4.add(1);
        inList4.add(106);
        inList4.add(100);
        List<Integer> lst5 = OptBdEdBaseBrandController.getBrandIds(inList3,inList4);
        for(Integer i:lst5){
            System.out.println(i);
        }

    }
}
