package ezr.bigdata.db.bigdata;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdJobStatusBean;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/14 14:50
 */
public class OptBdJobStatus {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.bigDataConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("opt_bd_job_status");

    /**
     * 拿到需要计算的品牌
     * select Id,BrandId,Comment  from opt_bd_job_status
     *  where name = 'crm_rule_vip_history_cfg_count'
     *  and dataCenter = 'test'
     *  and status=0 and
     *  BrandId in (-100000,128,24,310)
     *
     * @param list
     * @param dataCenter
     * @return
     *
     */

    public static Map<Integer, DateTime[]> getBrandIdTimesMapOfZero(List<Integer> list, String dataCenter) {
        DateTimeZone dtz = DateTimeZone.getDefault();
        Map<Integer, DateTime[]> map = new HashMap<Integer, DateTime[]>();
        String inStr = EzrStringUtil.getInStr(list);
        String sqlStr = "select Id,BrandId,Comment  from " + TABLE + " where name = 'crm_rule_vip_history_cfg_count' and dataCenter = '" + dataCenter + "' and status=0 and  BrandId in " + inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdJobStatusBean> beanList = new ArrayList<OptBdJobStatusBean>();
        try {
            while (rs.next()) {
                OptBdJobStatusBean objsb = new OptBdJobStatusBean();
                objsb.setId(rs.getInt("Id"));
                objsb.setBrandId(rs.getInt("BrandId"));
                objsb.setComment(rs.getString("Comment"));
                beanList.add(objsb);

            }
            JSONObject json = null;
            for (OptBdJobStatusBean bean : beanList) {
                DateTime[] arr = new DateTime[2];
                int brandid = bean.getBrandId();
                String jsonStr = bean.getComment();
                System.out.println("brandid " + brandid + " jsonStr " + jsonStr);
                json = new JSONObject(jsonStr);
                String rTimeTmp = json.getString("RegTime");
                String sTimeTmp = json.getString("SaleTime");
                LocalDateTime rTime = new LocalDateTime(rTimeTmp, dtz);
                if (dtz.isLocalDateTimeGap(rTime)) {
                    rTime = rTime.plusHours(1);
                }
                DateTime regTime = rTime.toDateTime();
                DateTime saleTime = null;

                if (null != sTimeTmp && !"0001-01-01".equals(sTimeTmp)) {
                    LocalDateTime sTime = new LocalDateTime(sTimeTmp, dtz);
                    if (dtz.isLocalDateTimeGap(sTime)) {
                        sTime = sTime.plusHours(1);
                    }
                    saleTime = sTime.toDateTime();
                }
                arr[0] = regTime;
                arr[1] = saleTime;
                map.put(brandid, arr);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }


        return map;
    }


    /**
     * 根据dataCenter拿到品牌id
     * select BrandId from opt_bd_job_status
     *  where status = 0
     *  and name = 'crm_rule_vip_history_cfg_count'
     *  and dataCenter = 'test'
     * @param dataCenter
     * @return
     */
    public static List<Integer> getBrandIds(String dataCenter) {
        String sqlStr = " select BrandId from " + TABLE + " where status = 0 and name = 'crm_rule_vip_history_cfg_count' and dataCenter = '" + dataCenter + "'";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs, "BrandId");
    }

    /**
     * 计算完成修改状态表
     * 一个品牌一个品牌的修改
     *
     * update opt_bd_job_status set status = 1 , ValidVipCount = 10001
     *  where Name = 'crm_rule_vip_history_cfg_count'
     *  and status = 0 and dataCenter = 'test'
     *  and BrandId =295
     * @param map
     * @param dataCenter
     *
     */
    public static void updateStatus(Map<Integer, Integer> map, String dataCenter) {
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            int key = entry.getKey();
            int value = entry.getValue();
            String sqlStr = "update " + TABLE + " set status = 1 , ValidVipCount = " + value + " where Name = 'crm_rule_vip_history_cfg_count' and status = 0 and dataCenter = '" + dataCenter + "' and BrandId =" + key;
            System.out.println(sqlStr);
            dbHelper.executeNonQuery(sqlStr);
        }

    }

    /**
     * 历史会员线上化获取brandid对应的上线时间和用户定义时间
     * select Id,BrandId,Comment  from opt_bd_job_status
     *  where id in (
     *              select max(id) from opt_bd_job_status
     *                  where name = 'crm_rule_vip_history_cfg_count'
     *                  and dataCenter = 'test'
     *                  and BrandId in (-100000,1,24)
     *                  GROUP BY BrandId
     *               )
     * @param list
     * @param dataCenter
     * @return
     */
    public static Map<Integer, DateTime[]> getBrandIdTimesMap(List<Integer> list, String dataCenter) {
        DateTimeZone dtz = DateTimeZone.getDefault();
        Map<Integer, DateTime[]> map = new HashMap<Integer, DateTime[]>();
        String inStr = EzrStringUtil.getInStr(list);
        String sqlStr = "select Id,BrandId,Comment  from " + TABLE + " where id in (select max(id) from opt_bd_job_status where name = 'crm_rule_vip_history_cfg_count' and dataCenter = '" + dataCenter + "' and BrandId in " + inStr + " GROUP BY BrandId)";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdJobStatusBean> beanList = new ArrayList<OptBdJobStatusBean>();
        try {
            while (rs.next()) {
                OptBdJobStatusBean objsb = new OptBdJobStatusBean();
                objsb.setId(rs.getInt("Id"));
                objsb.setBrandId(rs.getInt("BrandId"));
                objsb.setComment(rs.getString("Comment"));
                beanList.add(objsb);

            }

            JSONObject json = null;
            for (OptBdJobStatusBean bean : beanList) {
                DateTime[] arr = new DateTime[2];
                int brandid = bean.getBrandId();
                String jsonStr = bean.getComment();
                System.out.println(jsonStr);
                json = new JSONObject(jsonStr);
                String rTimeTmp = json.getString("RegTime");
                String sTimeTmp = json.getString("SaleTime");
                LocalDateTime rTime = new LocalDateTime(rTimeTmp, dtz);
                if (dtz.isLocalDateTimeGap(rTime)) {
                    rTime = rTime.plusHours(1);
                }
                DateTime regTime = rTime.toDateTime();
                DateTime saleTime = null;

                if (null != sTimeTmp && !"0001-01-01".equals(sTimeTmp)) {
                    LocalDateTime sTime = new LocalDateTime(sTimeTmp, dtz);
                    if (dtz.isLocalDateTimeGap(sTime)) {
                        sTime = sTime.plusHours(1);
                    }
                    saleTime = sTime.toDateTime();
                }
                arr[0] = regTime;
                arr[1] = saleTime;
                map.put(brandid, arr);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }

        return map;
    }

    /**
     * 拿到品牌计算历史会员线上化的计算时间，这之后绑定卡号的会员才会被认为是被激活的历史会员
     * select BrandId,UpdateTime FROM opt_bd_job_status
     *  where id in (
     *              SELECT Max(id) FROM opt_bd_job_status
     *              where name = 'crm_rule_vip_history_cfg_count'
     *              and BrandId in (-100000,1,24)
     *              and dataCenter = 'test'
     *              group by BrandId
     *              )
     *
     * @param list
     * @param dataCenter
     * @return
     */
    public static Map<Integer,DateTime> getHistoryVipOnlineComputeTimes(List<Integer> list, String dataCenter) {
        String inStr = EzrStringUtil.getInStr(list);
        String sqlStr = "select BrandId,UpdateTime FROM "+TABLE+" where id in (SELECT Max(id) FROM opt_bd_job_status where name = 'crm_rule_vip_history_cfg_count' and BrandId in "+inStr+" and dataCenter = '"+dataCenter+"' group by BrandId)";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        DateTimeZone dtz = DateTimeZone.getDefault();
        Map<Integer,DateTime>map = new HashMap<Integer,DateTime>();
        List<OptBdJobStatusBean> beanList = new ArrayList<OptBdJobStatusBean>();
        try {
            while (rs.next()) {
                OptBdJobStatusBean objsb = new OptBdJobStatusBean();
                objsb.setBrandId(rs.getInt("BrandId"));
                objsb.setUpdateTime(rs.getString("UpdateTime"));
                beanList.add(objsb);
            }

            if(beanList  != null && !beanList.isEmpty()){
                for(OptBdJobStatusBean bean :beanList){
                    String  updateStr = bean.getUpdateTime().substring(0,10);
                    LocalDateTime update = new LocalDateTime(updateStr, dtz);
                    if (dtz.isLocalDateTimeGap(update)) {
                        update = update.plusHours(1);
                    }

                    DateTime updateDT = update.toDateTime();
                    map.put(bean.getBrandId(), updateDT);
                }
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }


        return map;
    }

    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(128);
        inList.add(24);
        inList.add(310);

        /** 1*/
        Map<Integer, DateTime[]> map = OptBdJobStatus.getBrandIdTimesMapOfZero(inList, "test");
        for (Map.Entry<Integer, DateTime[]> entry : map.entrySet()) {
            Integer mapKey = entry.getKey();
            DateTime[] dt = entry.getValue();
            System.out.print("\n" + mapKey + ": \t");
            for (DateTime d : dt) {
                if (null != d) {
                    System.out.print(d.toString("yyyyMMdd") + "\t ");
                }

            }
        }
        /** 2*/
        List<Integer> brandids = OptBdJobStatus.getBrandIds("test");
        for (int brand : brandids) {
            System.out.print(brand + "\t");
        }

        /** 3 */
               Map<Integer, Integer> map2 = new HashMap<Integer, Integer>();
        map2.put(24, 10002);
        map2.put(313, 10001);
        map2.put(295, 10001);
        OptBdJobStatus.updateStatus(map2, "test");
        /** 3 */

        List<Integer> inList2 = new ArrayList();
        inList2.add(1);
        inList2.add(24);
        Map<Integer, DateTime[]> map3 = OptBdJobStatus.getBrandIdTimesMap(inList2, "test");
        for (Map.Entry<Integer, DateTime[]> entry : map3.entrySet()) {
            Integer mapKey = entry.getKey();
            DateTime[] dt = entry.getValue();
            System.out.print("\n" + mapKey + ": \t");
            for (DateTime d : dt) {
                if (null != d) {
                    System.out.print(d.toString("yyyyMMdd") + "\t ");
                }

            }
        }

        /** 4 */

        List<Integer> inList3 = new ArrayList();
        inList3.add(1);
        inList3.add(24);
        Map<Integer, DateTime> map4 = OptBdJobStatus.getHistoryVipOnlineComputeTimes(inList2, "test");
        for (Map.Entry<Integer, DateTime> entry : map4.entrySet()) {
            Integer mapKey = entry.getKey();
            DateTime dt = entry.getValue();
            System.out.print("\n" + mapKey + ": \t"+dt.toString("yyyyMMdd"));

        }
    }
}
