package ezr.es.opt;//package ezr.es.opt;
//
//import ezr.common.mybatis.controller.OptBdShardCfgController;
//import ezr.common.mybatis.controller.OptJobLastChangeController;
//import ezr.common.util.ConfigHelper;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * Auther: lorne
// * Date: 2018/6/6 14:51
// */
//public class RemoveOldESDateByMinRegTime {
//
//    public static void main(String[] args) {
//
//
//        String shardingId = args[0];
//        String index = "viprecruit"+shardingId;
//        String path = args[1];
//
//        String indexSid =index +  shardingId;
//        //String indexSid1001 =index + "1001";
//
//        String regTime = OptJobLastChangeController.getMinRegTime("hive_VipRecruit" + shardingId);
//
//        int dayInt = Integer.valueOf(regTime.substring(0,10).replace("-",""));
//
//        int fieldValue = dayInt;
//
//        String clusterName = ConfigHelper.getKeyValue("ezr.es.cluster_name");
//        String dataCenter = OptBdShardCfgController.getDataCenter(Integer.valueOf(shardingId));
//        //根据shaGrpId获取es服务器地址
//        String[] servers = OptBdShardCfgController.getESServers(Integer.valueOf(shardingId),dataCenter);
//        String esIp = servers[0];
////        String esPort = servers[1];
//        RemoveOldESDateByMinRegTime.deleteByQueryByShell(index,  fieldValue, clusterName, esIp, path);
//
//    }
//
//    public static void deleteByQueryByShell(String index, int fieldValue, String clusterName, String hostName, String filePath) {
//        Logger log = LoggerFactory.getLogger(RemoveOldESDateByMinRegTime.class);
//        log.info("---------------输出内容-----------------");
//
//        System.out.println("获取 transportClient " + clusterName + "  "+ hostName);
//
//        String command = "sh "+ filePath + "  " +  index  +"  "+ fieldValue;
//
//        System.out.println(command);
//        try {
//            Runtime runtime = Runtime.getRuntime();
//            log.info("runtime:"+runtime);
//
//            int exitValue =  runtime.exec(command).waitFor();
//            if (0 != exitValue) {
//                log.info("call shell failed. error code is :" + exitValue);
//                log.info("执行删除失败" + exitValue);
//            }else{
//                log.info("执行删除成功");
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            log.info("执行删除失败");
//        }
//
//    }
//}
