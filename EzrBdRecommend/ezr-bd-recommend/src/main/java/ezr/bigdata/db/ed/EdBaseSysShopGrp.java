package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdEdBaseSysShopGrpBean;
import ezr.common.mybatis.bean.ShopGrpNameAndIdBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 店群和门店
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 13:53
 */
public class EdBaseSysShopGrp {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE_GRP_DTL = ConfigHelper.getKeyValue("ed_base_sys_shop_grp_dtl");
    private static final String TABLE_GRP = ConfigHelper.getKeyValue("ed_base_sys_shop_grp");
//    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");

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
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "SELECT a.ShopId ShopId,b.NAME name,b.BrandId BrandId,a.ShopGrpId ShopGrpId FROM "+TABLE_GRP_DTL+" a LEFT JOIN "+TABLE_GRP+" b on b.id=a.ShopGrpId where b.BrandId in"+inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdEdBaseSysShopGrpBean> result = new ArrayList<OptBdEdBaseSysShopGrpBean>();
        try {
            while (rs.next()){
                OptBdEdBaseSysShopGrpBean ssg = new OptBdEdBaseSysShopGrpBean();
                ssg.setBrandId(rs.getInt("BrandId"));
                ssg.setShopGrpId(rs.getInt("ShopGrpId"));
                ssg.setShopId(rs.getInt("ShopId"));
                ssg.setName(rs.getString("name"));
                result.add(ssg);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return result;
    }


    /**
     * 获取门店id,店群名称，品牌id，店群id
     * 封装到map里
     * map<branid_shopId,(店群id,店群名称)>
     * @param brands
     * @return
     */
    public static Map<String, ShopGrpNameAndIdBean> getByShopGrpName(List<Integer> brands){
        Map<String, ShopGrpNameAndIdBean> map = new HashMap<String, ShopGrpNameAndIdBean>();
        List<OptBdEdBaseSysShopGrpBean> list = getByShopName(brands);
        String name="";
        String brandAndId="";
        if(!brands.isEmpty()){
            for(OptBdEdBaseSysShopGrpBean bean :list){
                Integer shopGrpId = bean.getShopGrpId();
                Integer brand = bean.getBrandId();
                Integer serviceId=bean.getShopId();
                if("".equals(bean.getName())||bean.getName()==null){
                    name="null";
                }else {
                    name=bean.getName();
                }
                brandAndId=brand+"_"+serviceId;
                map.put(brandAndId,new ShopGrpNameAndIdBean(name,shopGrpId));//(brand+"_"+serviceId,(name,rtlDistrictId))
            }

        }
        return map;
    }

    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdEdBaseSysShopGrpBean> lst = EdBaseSysShopGrp.getByShopName(inList);
        for(OptBdEdBaseSysShopGrpBean ssg:lst){
            System.out.println(ssg.getBrandId()+"\t"+ssg.getShopGrpId()+"\t"+ssg.getName()+"\t"+ssg.getShopId());
        }

        Map<String, ShopGrpNameAndIdBean> m = EdBaseSysShopGrp.getByShopGrpName(inList);
        for(Map.Entry<String, ShopGrpNameAndIdBean> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue().getShopGrpId()+e.getValue().getName());
        }
    }
}
