package io.mycat.mycat2.ymlTest;

import java.util.List;

/**
 * Desc:
 *
 * @date: 09/09/2017
 * @author: gaozhiwen
 */
public class RepBean {
    public enum RepTypeEnum {
        MASTER_SLAVE(0), MASTER_MASTER(1);
        private int code;
        RepTypeEnum(int code) {
            this.code = code;
        }
    }

    public enum RepSwitchTypeEnum {
        READ_ONLY(0), WRITE_ONLY(1);
        private int code;
        RepSwitchTypeEnum(int code) {
            this.code = code;
        }
    }

    private String name;
    private RepTypeEnum type;
    private RepSwitchTypeEnum switchType;
    private List<MySQL> mysqls;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RepTypeEnum getType() {
        return type;
    }

    public void setType(RepTypeEnum type) {
        this.type = type;
    }

    public RepSwitchTypeEnum getSwitchType() {
        return switchType;
    }

    public void setSwitchType(RepSwitchTypeEnum switchType) {
        this.switchType = switchType;
    }

    public List<MySQL> getMysqls() {
        return mysqls;
    }

    public void setMysqls(List<MySQL> mysqls) {
        this.mysqls = mysqls;
    }

    @Override public String toString() {
        return "RepBean{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", switchType=" + switchType +
                ", mysqls=" + mysqls +
                '}';
    }
}
