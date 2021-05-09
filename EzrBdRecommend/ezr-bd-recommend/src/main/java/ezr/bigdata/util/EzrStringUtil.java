package ezr.bigdata.util;

import java.util.List;

/**
 * @author liuchangfu@easyretailpro.com
 * @version 1.0
 * @date 2020/6/14 14:25
 */
public class EzrStringUtil {
    /**
     * 根据传入的List 构造SQL xxx in (x,y,z,g)
     * 括号里的部分
     * @param inList
     * @return
     */
    public static String getInStr(List<Integer> inList){
        String inStr = "(-100000";
        if(inList != null && inList.size()>0){
            for(int str:inList){
                inStr += ","+str;
            }
        }
        inStr += ")";
        System.out.println(inStr);
        return inStr;
    }

}
