package io.mycat.config.user;

import java.util.List;

/**
 * Desc: 黑白名单配置类
 *
 * @date: 29/09/2017
 * @author: gaul
 */
public class FireWallConfig {
    private boolean enable;
    private List<WhiteBean> white;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public List<WhiteBean> getWhite() {
        return white;
    }

    public void setWhite(List<WhiteBean> white) {
        this.white = white;
    }


    public static class WhiteBean {
        private String host;
        private String user;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
    }
}
