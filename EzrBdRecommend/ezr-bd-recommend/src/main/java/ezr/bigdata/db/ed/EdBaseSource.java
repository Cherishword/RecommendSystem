package ezr.bigdata.db.ed;

import ezr.common.util.ConfigHelper;
import ezr.bigdata.db.DBHelper;
import ezr.common.mybatis.bean.OptBdEdBaseSourceBean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/14 21:47
 */
public class EdBaseSource {
    private static final DBHelper dbHelper = new DBHelper(ConfigHelper.edConnection.replace("%", "%25"));
    private static final String TABLE = ConfigHelper.getKeyValue("ed_base_source");
//    private static final String RUN_ENV = ConfigHelper.getKeyValue("bigData.deployment.env");

    /** 获取 SourceVal,Sort并且是排序后的
     * SELECT SourceVal,Sort FROM ed_base_source
     *  where Sort>=1 ORDER By Sort
     * @return
     */
    public static List<OptBdEdBaseSourceBean> getEdBaseSource() {
        String sqlStr = "SELECT SourceVal,Sort FROM "+TABLE+" where Sort>=1 ORDER By Sort";
        System.out.println(sqlStr);
        ResultSet rs = dbHelper.query(sqlStr);
        List<OptBdEdBaseSourceBean> result = new ArrayList<OptBdEdBaseSourceBean>();
        try {
            while (rs.next()){
                OptBdEdBaseSourceBean ebsb = new OptBdEdBaseSourceBean();
                ebsb.setSourceVal(rs.getString("SourceVal"));
                ebsb.setSort(rs.getInt("Sort"));
                result.add(ebsb);
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
     * 注意这个方法名字起得容易歧义，其实并不是在排序。只是把List 变成Map
     *  把 SourceVal,Sort 让如map
     *  map<SourceVal,Sort>
     * @return
     */
    public static Map<String,Integer> getSortId(){
        Map<String, Integer> map = new HashMap<String, Integer>();
        List<OptBdEdBaseSourceBean> list = getEdBaseSource();
        Integer id=0;
        String sourceVal="";

        for (int i = 0; i < list.size(); i++) {

            id = list.get(i).getSort();
            sourceVal = list.get(i).getSourceVal();
            map.put(sourceVal,id);
        }
        return map;
    }

}
