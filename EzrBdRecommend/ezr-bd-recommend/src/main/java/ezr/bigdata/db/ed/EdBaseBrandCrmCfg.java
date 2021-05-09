package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/14 19:50
 */
public class EdBaseBrandCrmCfg {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("bd_ed_base_brand_crm_cfg");
    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");
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
    public static List<Integer> getBrandIds(List<Integer> brandIds, int vipSalStaticsType){
        String inStr = EzrStringUtil.getInStr(brandIds);
        String sqlStr = "select BrandId from "+TABLE+" where vipSalStaticsType = "+vipSalStaticsType+"  and brandId in"+inStr;
        System.out.println(sqlStr);
        ResultSet rs =  dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs,"BrandId");
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
        String inStr = EzrStringUtil.getInStr(brandIds);
        String sqlStr = "";
        /**私有化部署执行if 条件的 sql 或者默认执行普通部署方式区分datacenter*/
        if("private".equals(RUN_ENV)){
            sqlStr = "select brandId from "+TABLE+" where IsNoVipOrder = "+isNoVipOrder+" and brandId in"+inStr;
        }else {
            sqlStr = "select brandId from "+TABLE+" where IsNoVipOrder = "+isNoVipOrder+" and brandId in"+inStr+" and dataCenter='"+dataCenter+"'";
        }
        System.out.println(sqlStr);
        ResultSet rs =  dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs,"brandId");
    }

    public static void main(String[] args){
        /**1*/
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<Integer>  lst1 = EdBaseBrandCrmCfg.getBrandIds(inList,1);
        for(int i:lst1){
            System.out.println(i);
        }

        List<Integer>  lst2 = EdBaseBrandCrmCfg.getBrandIdsByIsNoVipOrder(inList,"1","test");
        for(int i:lst2){
            System.out.println(i);
        }
    }
}
