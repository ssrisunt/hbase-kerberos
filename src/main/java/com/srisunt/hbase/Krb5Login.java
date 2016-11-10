package com.srisunt.hbase;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;

/**
 * Created by ssrisunt on 10/17/16.
 */
public class Krb5Login {
    public static void main(String[] args) {
        Subject subject = login("ssrisunt@EXAMPLE.COM","yong2904");
    }
    public static Subject login(String user, String pwd) {
        String userName = user;
        char[] password = pwd.toCharArray();
        System.setProperty("java.security.krb5.conf", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/save/jass.conf");

        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc", "sandbox.hortonworks.com");
        try {
            LoginContext lc = new LoginContext("client", new UserNamePasswordCallbackHandler(userName, password));
            lc.login();
            System.out.println("KerberosAuth.main: " + lc.getSubject());
            return lc.getSubject();
        } catch (LoginException le) {
            le.printStackTrace();
        }
        return null;
    }

    public static class UserNamePasswordCallbackHandler implements CallbackHandler {
        private String _userName;
        private char[] _password;

        public UserNamePasswordCallbackHandler(String userName, char[] password) {
            _userName = userName;
            _password = password;
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback && _userName != null) {
                    ((NameCallback) callback).setName(_userName);
                } else if (callback instanceof PasswordCallback && _password != null) {
                    ((PasswordCallback) callback).setPassword(_password);
                }
            }
        }
    }

}
