package org.telegram.mtproto.transport;

/**
 * Created by ex3ndr on 26.11.13.
 */
public class ConnectionType {
    public static final int TYPE_TCP = 0;

    private String host;
    private int port;
    private int connectionType;

    public ConnectionType(String host, int port, int connectionType) {
        this.host = host;
        this.port = port;
        this.connectionType = connectionType;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getConnectionType() {
        return connectionType;
    }
}
