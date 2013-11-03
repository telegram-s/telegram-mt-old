package org.telegram.mtproto;

import org.telegram.mtproto.transport.TcpContext;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 02.11.13
 * Time: 21:25
 */
public class Main {
    public static void main(String[] args) throws IOException {
        Authorizer authorizer = new Authorizer();
        authorizer.doAuth();
    }
}
