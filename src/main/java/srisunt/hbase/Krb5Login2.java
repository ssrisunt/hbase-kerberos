package com.srisunt.hbase;

import org.ietf.jgss.*;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created by ssrisunt on 10/17/16.
 */
public class Krb5Login2 {
    public static void main(String[] args) {
        Subject subject = login();
    }

    public static Subject login() {
        String userName = "ssrisunt@EXAMPLE.COM";
        char[] password = "yong2904".toCharArray();
        System.setProperty("java.security.krb5.conf", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/save/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/save/jass.conf");
        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc", "sandbox");

        String servicePrincipalName = "sandbox";


        try {
            LoginContext lc = new LoginContext("client", new UserNamePasswordCallbackHandler(userName, password));

            lc.login();

            Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");


            Subject subject = lc.getSubject();
             byte[] serviceTicket;

            GSSManager manager = GSSManager.getInstance();
            GSSName serverName = manager.createName( servicePrincipalName,
                    GSSName.NT_HOSTBASED_SERVICE);
            final GSSContext context = manager.createContext( serverName, krb5Oid, null,
                    GSSContext.DEFAULT_LIFETIME);
            // The GSS context initiation has to be performed as a privileged action.
            serviceTicket = Subject.doAs( subject, new PrivilegedAction<byte[]>() {
                public byte[] run() {
                    try {
                        byte[] token = new byte[0];
                        // This is a one pass context initialisation.
                        context.requestMutualAuth( false);
                        context.requestCredDeleg( false);
                        return context.initSecContext( token, 0, token.length);
                    }
                    catch ( GSSException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            });

            System.out.println("KerberosAuth.main: " + lc.getSubject());
            return lc.getSubject();
        } catch (LoginException le) {
            le.printStackTrace();
        } catch (GSSException e) {
            e.printStackTrace();
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
