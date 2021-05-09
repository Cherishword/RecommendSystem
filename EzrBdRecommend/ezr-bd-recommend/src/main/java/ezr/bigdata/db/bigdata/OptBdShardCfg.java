package ezr.bigdata.db.bigdata;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.bigdata.util.EzrStringUtil;
import ezr.common.mybatis.bean.OptBdShardCfgBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/13 19:46
 */
public class OptBdShardCfg {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.bigDataConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("opt_bd_shard_cfg");

    /**
     * 拿到表opt_bd_shard_cfg的所有字段
     * @return
     */
    public static List<OptBdShardCfgBean> selectAll(){
        List<OptBdShardCfgBean> list = new ArrayList<OptBdShardCfgBean>();
        ResultSet rs = dbHelper.query("select * from "+TABLE);
        try {
            while (rs.next()) {
                OptBdShardCfgBean obscbeab = new OptBdShardCfgBean();
                obscbeab.setId(rs.getInt("Id"));
                obscbeab.setDataCenter(rs.getString("DataCenter"));
                obscbeab.setEsId(rs.getInt("ESId"));
                obscbeab.setShardingId(rs.getInt("ShardingId"));
                obscbeab.setShardingGrpId(rs.getInt("ShardingGrpId"));
                list.add(obscbeab);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return list;
    }
    /**根据shardingGrpId和dataCenter，得到Es服务器地址*/
    public  static String[] getESServers(int shardingGrpId, String dataCenter){
        String[] esHostAndPort = new String[2];
        String sqlStr = "SELECT es.ESHost ESHost FROM "+TABLE+" cfg LEFT JOIN opt_bd_es es ON cfg.ESId = es.Id WHERE shardingGrpId = "+shardingGrpId+" and dataCenter = '"+dataCenter+"' limit 1";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        String result= dbHelper.getStringResult(rs,"ESHost");

        if(null == result || result.length() <=0){
            System.exit(-1); //如果es服务地址找不到则结束程序
        }else{
            String[] servers = result.split(",");
//            for (String server:servers) {
//                System.out.println("server "+server);
//            }

            String esPort = "9200";
            String esIp = "";


            for (int i = 0 ;i <  servers.length ;i++) {
                String[] ip  = servers[i].split(":");

                if (i != servers.length - 1) {
                    esIp += ip[0] + ",";
                } else {
                    esIp += ip[0];
                    esPort = ip[1];
                }
            }

            esHostAndPort[0] = esIp;
            esHostAndPort[1] = esPort;
            System.out.println(esHostAndPort[0]+" <====> "+esHostAndPort[1]);
        }
        return esHostAndPort;
    }

    /**
     * 根据ShardingGrpId 取出其包含的多个shardingId的值
     * @param shardingGrpId
     * @param dataCenter
     * @return
     */
    public static List<Integer>  getShardingIds(int shardingGrpId, String dataCenter){
        String sqlStr = "select ShardingId from "+TABLE+" where ShardingGrpId = "+shardingGrpId+" and dataCenter = '"+dataCenter+"'";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResultList(rs,"ShardingId");
    }

    public static  String getDataCenter(int shardingGrpId){
        String sqlStr = "select DataCenter from "+TABLE+" where ShardingGrpId = "+shardingGrpId+" limit 1";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getStringResult(rs,"DataCenter");
    }

    /**
     * 根据传入的shardingGrpId获取对应的ShardingGrpId和DataCenter
     * @param shardingGrpId
     * @return 返回 OptBdShardCfgBean对象，切只有ShardingGrpId,DataCenter有实际值
     */
    public static  List<OptBdShardCfgBean> getShardIngAndDataCenter(int shardingGrpId){
        String sqlStr = "select ShardingGrpId,DataCenter from "+TABLE+" where ShardingGrpId = "+shardingGrpId+" limit 1";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdShardCfgBean> rssult = new ArrayList<OptBdShardCfgBean>();

        try {
            while (rs.next()){
                OptBdShardCfgBean obsc = new OptBdShardCfgBean();
                obsc.setShardingId( rs.getInt("ShardingGrpId"));
                obsc.setDataCenter(rs.getString("DataCenter"));
                obsc.setShardingGrpId(shardingGrpId);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return rssult;
    }
    /**根据shardingId和dataCenter，得到shardingGrpId*/
    public static  int  getShardGrpId(int shardingId,String dataCenter){
        String sqlStr = "select ShardingGrpId from "+TABLE+" where ShardingId = "+shardingId+" and dataCenter = '"+dataCenter+"'";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        return dbHelper.getIntResult(rs,"ShardingGrpId");
    }
    /**根据shardingId获取shardGrpid和datacenter*/
    public static  List<OptBdShardCfgBean>   getShardGrpAndDataCenter(List<Integer> shardingIds){
        String inStr = EzrStringUtil.getInStr(shardingIds);
        String sqlStr = "select ShardingId,ShardingGrpId,DataCenter FROM "+TABLE+" where ShardingId in "+inStr;
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdShardCfgBean> list = new ArrayList<OptBdShardCfgBean>();
        try {
            while (rs.next()) {
                OptBdShardCfgBean obscbeab = new OptBdShardCfgBean();
                obscbeab.setShardingId(rs.getInt("ShardingId"));
                obscbeab.setShardingGrpId(rs.getInt("ShardingGrpId"));
                obscbeab.setDataCenter(rs.getString("DataCenter"));
                list.add(obscbeab);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            dbHelper.free(rs);
            dbHelper.free(dbHelper.conn);
        }
        return list;
    }


    public static void main(String[] args){
        List<Integer> inList = new ArrayList();
        inList.add(1);
        inList.add(100);
        inList.add(234);
        EzrStringUtil.getInStr(inList);

//        String[] esHostAndPort= OptBdShardCfg.getESServers(1,"test");
//        List<Integer> intResultList = OptBdShardCfg.getShardingIds(1,"test");
//        for(Integer i :intResultList){
//            System.out.println(i);
//        }
//        System.out.println(OptBdShardCfg.getDataCenter(1));
//        System.out.println(OptBdShardCfg.getShardGrpId(1,"test"));

        List<OptBdShardCfgBean> list =OptBdShardCfg.getShardGrpAndDataCenter(inList);
        if (list != null){
            for(OptBdShardCfgBean b:list){
                System.out.println(
                        b.getId()+"\t"
                                +b.getEsId()+"\t"
                                +b.getDataCenter()+"\t"
                                +b.getShardingId()+"\t"
                                +b.getShardingGrpId()+"\t"+b.getClusterId());
            }
        }
    }
}
