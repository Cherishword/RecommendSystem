package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.EdBaseBrand;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/14 16:53
 */
public class EdBaseBrandDB {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("ed_base_brand");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");


    /**
     * 根据 CrmDbShardingId 获取 活跃的品牌编号
     *select Id FROM ed_base_brand
     *      where IsActive = 1
     *      and CrmDbShardingId in (-100000,1,100)
     * @param shardingIds
     * @return
     */
    public static List<Integer> getBrandIds(List<Integer> shardingIds){
        ArrayList<Integer> list = null;
        String inStr = EzrStringUtil.getInStr(shardingIds);
        String sqlStr = "select Id FROM "+TABLE+" where IsActive = 1 and CrmDbShardingId in "+inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs,"Id");
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
        String inStr1DbSharding = EzrStringUtil.getInStr(shardingIds);
        String inStr2BrandId = EzrStringUtil.getInStr(brandIds);
        String sqlStr = " select Id FROM "+TABLE+"  where CrmDbShardingId in"+inStr1DbSharding+" and Id in "+inStr2BrandId;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs,"Id");
    }

    /**
     * 获取品牌的CrmDbShardingId
     *select CrmDbShardingId FROM ed_base_brand
     *  where id = 1
     * @param brandId
     * @return
     */
    public static int getShardingId(int brandId){
        String sqlStr = "select CrmDbShardingId FROM "+TABLE+" where id = "+brandId;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResult(rs,"CrmDbShardingId");
    }

    /**
     * 查询所有没有流失的品牌
     * SELECT Id,Code,CrmDbShardingId,Name FROM ed_base_brand
     *  where IsActive = 1
     * @return
     */
    public static ArrayList<EdBaseBrand> getAllActiveBrand(){
        String sqlStr = "SELECT Id,Code,CrmDbShardingId,Name FROM "+TABLE+" where IsActive = 1";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        ArrayList<EdBaseBrand> lst   = new ArrayList<EdBaseBrand>();
        try {
            while (rs.next()){
                EdBaseBrand ebb = new EdBaseBrand();
                ebb.setId(rs.getInt("Id"));
                ebb.setCode(rs.getString("Code"));
                ebb.setCrmDbShardingId(rs.getInt("CrmDbShardingId"));
                ebb.setName(rs.getString("Name"));
                lst.add(ebb);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }

        return lst;
    }

    /**
     * 查询所有已经流失的品牌信息
     * SELECT Id,Code,CrmDbShardingId,Name FROM ed_base_brand
     *  where IsActive = 0
     * @return
     */
    public static ArrayList<EdBaseBrand> getLapsedBrand(){
        String sqlStr = "SELECT Id,Code,CrmDbShardingId,Name FROM "+TABLE+" where IsActive = 0";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        ArrayList<EdBaseBrand> lst   = new ArrayList<EdBaseBrand>();
        try {
            while (rs.next()){
                EdBaseBrand ebb = new EdBaseBrand();
                ebb.setId(rs.getInt("Id"));
                ebb.setCode(rs.getString("Code"));
                ebb.setCrmDbShardingId(rs.getInt("CrmDbShardingId"));
                ebb.setName(rs.getString("Name"));
                lst.add(ebb);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }

        return lst;
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
        String inStr = EzrStringUtil.getInStr(shardingIds);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select Id FROM "+TABLE+" where IsActive = 1 and CrmDbShardingId in "+inStr;
        }else {
            sqlStr = "select Id FROM "+TABLE+" where IsActive = 1 and CrmDbShardingId in "+inStr+" and DataCenter = '"+dataCenter+"'";
        }
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs,"Id");
    }

    public static void main(String[] args){
        /** 1 */
        System.out.println(EdBaseBrandDB.getShardingId(1));

        /** 2 */
        ArrayList<EdBaseBrand> lst1   = EdBaseBrandDB.getAllActiveBrand();
        for(EdBaseBrand ebb:lst1){
            System.out.println(ebb.getId()+"\t"+ebb.getCode()+"\t"+ebb.getCrmDbShardingId()+"\t"+ebb.getName());
        }

        /** 3 */
        ArrayList<EdBaseBrand> lst2   = EdBaseBrandDB.getLapsedBrand();
        for(EdBaseBrand ebb:lst2){
            System.out.println(ebb.getId()+"\t"+ebb.getCode()+"\t"+ebb.getCrmDbShardingId()+"\t"+ebb.getName());
        }

        /** 4 */
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(100);
        List<Integer> lst3 = EdBaseBrandDB.getBrandIdsByDataCenter(inList,"test");
        for(Integer i:lst3){
            System.out.println(i);
        }

        /**5*/
        List<Integer> inList2 = new ArrayList();
        inList2.add(1);
        inList2.add(100);
        List<Integer> lst4 = EdBaseBrandDB.getBrandIds(inList2);
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
        List<Integer> lst5 = EdBaseBrandDB.getBrandIds(inList3,inList4);
        for(Integer i:lst5){
            System.out.println(i);
        }

    }

}
