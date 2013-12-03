package org.telegram.mtproto.state;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 07.11.13
 * Time: 7:26
 */
public class ConnectionInfo {
    private int id;
    private String address;
    private int port;

    public ConnectionInfo(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public int getId() {
        return id;
    }
}
