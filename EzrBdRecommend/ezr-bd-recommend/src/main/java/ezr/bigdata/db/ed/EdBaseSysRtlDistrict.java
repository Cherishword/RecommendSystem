package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdEdBaseSysRtlDistrictBean;
import ezr.common.mybatis.bean.RtlDistrictNameAndIdBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 片区各门店
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/15 11:46
 */
public class EdBaseSysRtlDistrict {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE_DISTRICT_DTL = ConfigHelper.getKeyValue("ed_base_sys_rtl_district_dtl");
    private static final String TABLE_DISTRICT = ConfigHelper.getKeyValue("ed_base_sys_rtl_district");
//    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");

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
        String inStr = EzrStringUtil.getInStr(brands);
        String sqlStr = "SELECT a.ShopId ShopId,b.NAME name,b.BrandId BrandId,b.id id FROM "+TABLE_DISTRICT_DTL+" a LEFT JOIN "+TABLE_DISTRICT+" b on b.id=a.RtlDistrictId where b.BrandId in "+inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdEdBaseSysRtlDistrictBean> result = new ArrayList<OptBdEdBaseSysRtlDistrictBean>();
        try {
            while (rs.next()){
                OptBdEdBaseSysRtlDistrictBean srd = new OptBdEdBaseSysRtlDistrictBean();
                srd.setId(rs.getInt("id"));
                srd.setShopId(rs.getInt("ShopId"));
                srd.setBrandId(rs.getInt("BrandId"));
                srd.setName(rs.getString("name"));
                result.add(srd);
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
     * 把得到的门店id,片区名称，品牌，片区id
     * 封装到map里
     *  map<品牌id_shopId,(片区id，片区名称)>
     * @param brands
     * @return
     */
    public static Map<String, RtlDistrictNameAndIdBean> getByRtlDistrictName(List<Integer> brands){
        Map<String, RtlDistrictNameAndIdBean> map = new HashMap<String, RtlDistrictNameAndIdBean>();
        List<OptBdEdBaseSysRtlDistrictBean> list = getByRtlName(brands);
        String name="";
        String brandAndId="";
        if(!brands.isEmpty()){
            for(OptBdEdBaseSysRtlDistrictBean bean :list){
                Integer rtlDistrictId = bean.getId();
                Integer brand = bean.getBrandId();
                Integer serviceId=bean.getShopId();
                if("".equals(bean.getName())||bean.getName()==null){
                    name="null";
                }else {
                    name=bean.getName();
                }
                brandAndId=brand+"_"+serviceId;
                map.put(brandAndId,new RtlDistrictNameAndIdBean(name,rtlDistrictId));//(brand+"_"+serviceId,(name,rtlDistrictId))
            }
        }
        return map;
    }

    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(2);
        inList.add(20);
        List<OptBdEdBaseSysRtlDistrictBean>  srd =  EdBaseSysRtlDistrict.getByRtlName(inList);
        for(OptBdEdBaseSysRtlDistrictBean s:srd){
            System.out.println(s.getId()+"\t"+s.getShopId()+"\t"+s.getName()+"\t"+s.getBrandId());
        }

        Map<String, RtlDistrictNameAndIdBean> m = EdBaseSysRtlDistrict.getByRtlDistrictName(inList);
        for(Map.Entry<String, RtlDistrictNameAndIdBean> e:m.entrySet()){
            System.out.println(e.getKey()+"\t"+e.getValue().getRtlDistrictId()+"\t"+e.getValue().getName());
        }
    }
}
