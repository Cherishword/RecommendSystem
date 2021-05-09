package ezr.common.mybatis.controller;


import ezr.bigdata.db.bigdata.OptBdJobStatus;
import org.joda.time.DateTime;
import java.util.*;

/**
 * Created by liucf on 2018/5/14.
 */
public class OptBdJobStatusController {
    /**
     * 根据dataCenter拿到品牌id
     * select BrandId from opt_bd_job_status
     *  where status = 0
     *  and name = 'crm_rule_vip_history_cfg_count'
     *  and dataCenter = 'test'
     * @param dataCenter
     * @return
     */
    public static List<Integer> getBrandIds(String dataCenter){

        return OptBdJobStatus.getBrandIds(dataCenter);
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

    public static void updateStatus(Map<Integer, Integer> map,String dataCenter){
        OptBdJobStatus.updateStatus(map,dataCenter);
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
    public static Map<Integer,DateTime[]> getBrandIdTimesMap(List<Integer> list,String dataCenter){
        return OptBdJobStatus.getBrandIdTimesMap(list,dataCenter);
    }

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
    public static Map<Integer,DateTime[]> getBrandIdTimesMapOfZero(List<Integer> list,String dataCenter){
        return OptBdJobStatus.getBrandIdTimesMapOfZero(list,dataCenter);
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
    public static Map<Integer,DateTime> getHistoryVipOnlineComputeTimes(List<Integer> list,String dataCenter){
        return OptBdJobStatus.getHistoryVipOnlineComputeTimes(list,dataCenter);
    }


    public static void main(String[] args) {
        List<Integer> inList = new ArrayList();
        inList.add(128);
        inList.add(24);
        inList.add(310);

        /** 1*/
        Map<Integer, DateTime[]> map = OptBdJobStatusController.getBrandIdTimesMapOfZero(inList, "test");
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
        List<Integer> brandids = OptBdJobStatusController.getBrandIds("test");
        for (int brand : brandids) {
            System.out.print(brand + "\t");
        }

        /** 3 */
        Map<Integer, Integer> map2 = new HashMap<Integer, Integer>();
        map2.put(24, 10002);
        map2.put(313, 10001);
        map2.put(295, 10001);
        OptBdJobStatusController.updateStatus(map2, "test");
        /** 3 */

        List<Integer> inList2 = new ArrayList();
        inList2.add(1);
        inList2.add(24);
        Map<Integer, DateTime[]> map3 = OptBdJobStatusController.getBrandIdTimesMap(inList2, "test");
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
        Map<Integer, DateTime> map4 = OptBdJobStatusController.getHistoryVipOnlineComputeTimes(inList2, "test");
        for (Map.Entry<Integer, DateTime> entry : map4.entrySet()) {
            Integer mapKey = entry.getKey();
            DateTime dt = entry.getValue();
            System.out.print("\n" + mapKey + ": \t"+dt.toString("yyyyMMdd"));

        }
    }
}
