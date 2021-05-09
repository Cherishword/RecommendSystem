package ezr.common.mybatis.controller;

import ezr.bigdata.db.bigdata.OptLastChange;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/13 19:14
 */
public class OptJobLastChangeController {
    /**
     * 根据jobName获取 LastModifyDate
     * @param jobName
     * @return
     */
    public static String getLastChangeDate(String jobName){
        return OptLastChange.getLastChangeDate(jobName);
    }

    /**
     * 根据jobName更新 LastModifyDate
     * @param jobName
     */
    public static void updateLastChange(String jobName){
        OptLastChange.updateLastChange(jobName);
    }

    /**
     * 根据jobName删除记录
     * @param jobName
     */
    public static void deleteJobName(String jobName){
        OptLastChange.deleteJobName(jobName);
    }

    public static void main(String[] args){
        System.out.println(OptJobLastChangeController.getLastChangeDate("hive_VipFirstSale1"));
        OptJobLastChangeController.updateLastChange("hive_VipFirstSale1");
        System.out.println(OptJobLastChangeController.getLastChangeDate("hive_VipFirstSale1"));
        OptJobLastChangeController.deleteJobName("hive_WeChat_HistoryVip1");
    }

}
