package dbs;

public class Finger {
    private Long id;
    String host;
    Integer port;

    public Finger(Long id, String host, Integer port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    Long getId() {
        return id;
    }

    String getHost() {
        return host;
    }

    Integer getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "Finger{" +
                "id=" + id +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
