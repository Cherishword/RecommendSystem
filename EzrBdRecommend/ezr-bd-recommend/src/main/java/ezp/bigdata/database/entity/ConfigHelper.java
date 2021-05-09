package ezp.bigdata.database.entity;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigHelper {
    static String profilepath = "application.conf";
    private static Properties props = new Properties();
    public static String optConnection;
    public static String esNodes;
    public static String hbaseZookeeperQuorum;


    public ConfigHelper() {
    }

    public static String getKeyValue(String key) {
        return props.getProperty(key);
    }

    public static void main(String[] args) {
        System.out.println(optConnection);
        System.out.println(esNodes);
        System.out.println(hbaseZookeeperQuorum);
    }

    static {
        try {
            InputStream is = ConfigHelper.class.getClassLoader().getResourceAsStream(profilepath);
            props.load(is);
        } catch (FileNotFoundException var1) {
            var1.printStackTrace();
            System.exit(-1);
        } catch (IOException var2) {
            System.exit(-1);
        }

        optConnection = getKeyValue("ezr.connection.opt");
        esNodes = getKeyValue("es.nodes");
        hbaseZookeeperQuorum = getKeyValue("hbase.zookeeper.quorum");

    }
}