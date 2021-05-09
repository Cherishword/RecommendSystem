package ezr.common.mybatis.bean;

import java.io.Serializable;

public class RtlDistrictNameAndIdBean implements Serializable {
    private String name;
    private Integer rtlDistrictId;

    public RtlDistrictNameAndIdBean(String name, Integer rtlDistrictId){
        this.name = name;
        this.rtlDistrictId = rtlDistrictId;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getRtlDistrictId() {
        return rtlDistrictId;
    }

    public void setRtlDistrictId(Integer rtlDistrictId) {
        this.rtlDistrictId = rtlDistrictId;
    }
}
