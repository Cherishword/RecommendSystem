package ezr.common.mybatis.bean;

import java.io.Serializable;

public class ShopGrpNameAndIdBean implements Serializable {
    private String name;
    private Integer shopGrpId;

    public ShopGrpNameAndIdBean(String name, Integer shopGrpId) {
        this.name = name;
        this.shopGrpId = shopGrpId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getShopGrpId() {
        return shopGrpId;
    }

    public void setShopGrpId(Integer shopGrpId) {
        this.shopGrpId = shopGrpId;
    }
}
