package ezr.common.mybatis.controller;

import ezr.bigdata.db.bigdata.OptBdMonitorJobResult;

/**
 * Created by liucf on 2018/9/14.
 * 对 opt_bd_monitor_job_result 库进行操作
 */
public class OptBdMonitorJobResultContorller {
    public static void updateMonitorResult(String jobName,int status){
        OptBdMonitorJobResult.updateMonitorResult(jobName,status);
    }

    public static void main(String[] args) {
        OptBdMonitorJobResultContorller.updateMonitorResult("WeChatActivatesHistoryVip-1",100);
    }
}
