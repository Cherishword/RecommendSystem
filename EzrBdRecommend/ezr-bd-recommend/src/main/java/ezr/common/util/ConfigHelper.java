package ezr.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by wangxmPC on 2016-05-10.
 * 解析配置文件
 */

public class ConfigHelper {
    static String profilepath="application.conf";

    private static Properties props = new Properties();
    static {
        try {
            InputStream is= ConfigHelper.class.getClassLoader().getResourceAsStream(profilepath);
            props.load(is);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            System.exit(-1);
        }
    }

    public static String getKeyValue(String key) {
        return props.getProperty(key);
    }

    public static String baseConnection = getKeyValue("ezr.connection.base");
    public static String edConnection = getKeyValue("ezr.connection.ed");
    public static String bigDataConnection = getKeyValue("ezr.connection.bigData");
    public static String crmConnection = getKeyValue("ezr.connection.crm");
    public static final String ENV = ConfigHelper.getKeyValue("ezr.bigdata.env");
    public static final String MYSQL_OPT = ConfigHelper.getKeyValue("ezr.connection.bigData");
    public static final String VIP_INFO = ".dw_ezp_crm_crm_vip_info";
    public static final String VIP_INFO_TEMP = ".crm_vip_info_temp";
    public static final String GRADE_LOG = ".dw_ezp_crm_crm_vip_info_grade_log";
    public static final String GRADE_LOG_TEMP = ".crm_vip_info_grade_log_temp";
    public static final String SER_COMMENT = ".dw_ezp_crm_crm_ser_comment";
    public static final String SER_COMMENT_TEMP = ".crm_ser_comment_temp";
    public static final String VIP_SALE = ".dw_ezp_crm_crm_sal_vip_sale";
    public static final String VIP_SALE_TEMP = ".crm_sal_vip_sale_temp";
    public static final String VIP_SALE_PROD = ".dw_ezp_crm_crm_sal_vip_sale_prod";
    public static final String VIP_SALE_PROD_TEMP = ".crm_sal_vip_sale_prod_temp";
    public static final String SHOP_SALE = ".dw_ezp_crm_crm_sal_shop_sale";
    public static final String SHOP_SALE_TEMP = ".crm_sal_shop_sale_temp";
    public static final String VIP_BONUS = ".dw_ezp_crm_crm_vip_info_bonus";
    public static final String VIP_BONUS_TEMP = ".crm_vip_info_bonus_temp";
    public static final String COUPON_LIST = ".dw_ezp_crm_crm_coupon_list";
    public static final String COUPON_LIST_TEMP = ".crm_coupon_list_temp";
    public static final String VIP_BIND_OLD = ".crm_vip_info_bindold_temp";
    public static final String BI_RFM = ".bi_rfm";//
    public static final String VIP_CONSUME = ".dw_ezp_crm_crm_vip_info_consume";
    public static final String OMS_SALE = ".dw_oms_oms_sale_order";

    public static void main(String[] args) {
        System.out.println(bigDataConnection);
        System.out.println(edConnection);
        System.out.println(crmConnection);
    }
}
