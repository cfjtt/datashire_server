package com.eurlanda.datashire.server.utils;

/**
 * 利用threadlocal来保存 token,key
 *
 * Created by zhudebin on 16/1/13.
 */
public class TokenUtil {

    private static ThreadLocal<TokenAndKey> tokenSet = new ThreadLocal<>();

    public static void setTokenAndKey(String token, String key) {
        TokenAndKey tk = tokenSet.get();
        if(tk == null) {
            tokenSet.set(new TokenAndKey(token, key));
        } else {
            tk.setToken(token);
            tk.setKey(key);
        }
    }

    public static void setToken(String token) {
        TokenAndKey tk = tokenSet.get();
        if(tk == null) {
            tokenSet.set(new TokenAndKey(token, null));
        } else {
            tk.setToken(token);
        }
    }

    public static void setKey(String key) {
        TokenAndKey tk = tokenSet.get();
        if(tk == null) {
            tokenSet.set(new TokenAndKey(null, key));
        } else {
            tk.setKey(key);
        }
    }

    public static String getToken() {
        TokenAndKey tk = tokenSet.get();
        if(tk != null) {
            return tk.getToken();
        } else {
            return null;
        }
    }

    public static String getKey() {
        TokenAndKey tk = tokenSet.get();
        if(tk != null) {
            return tk.getKey();
        } else {
            return null;
        }
    }

    public static void removeTokenAndKey() {
        tokenSet.remove();
    }

    private static class TokenAndKey {
        // 用户登录的验证信息
        private String token;
        // 一次接口调用的唯一ID
        private String key;

        public TokenAndKey(String token, String key) {
            this.token = token;
            this.key = key;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }
}
