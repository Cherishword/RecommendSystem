package ezr.common.mybatis.bean;

import java.io.Serializable;

public class OptBdEdBaseSysUser implements Serializable{

    private int id;
    private int brandId;
    private String userType;
    private String mobileNo;
    private int shopId;
    private String code;

    public OptBdEdBaseSysUser() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getBrandId() {
        return brandId;
    }

    public void setBrandId(int brandId) {
        this.brandId = brandId;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public String getMobileNo() {
        return mobileNo;
    }

    public void setMobileNo(String mobileNo) {
        this.mobileNo = mobileNo;
    }

    public int getShopId() {
        return shopId;
    }

    public void setShopId(int shopId) {
        this.shopId = shopId;
    }

    @Override
    public String toString() {
        return "OptBdEdBaseSysUser{" +
                "id=" + id +
                ", brandId=" + brandId +
                ", userType='" + userType + '\'' +
                ", mobileNo='" + mobileNo + '\'' +
                ", shopId=" + shopId +
                '}';
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
