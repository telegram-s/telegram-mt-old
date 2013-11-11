package org.telegram.mtproto.state;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 07.11.13
 * Time: 7:26
 */
public class ConnectionInfo {
    private String address;
    private int port;

    public ConnectionInfo(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }
}
