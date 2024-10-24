package modelos;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;

public class Conexion {
    private CqlSession session;

    public Conexion(String node, int port) {
        // Conectar a Cassandra
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(node, port))
                .withLocalDatacenter("datacenter1") // Cambia según tu configuración
                .build();
    }

    public CqlSession getSession() {
        return session;
    }

    public void cerrarConexion() {
        if (session != null) {
            session.close();
        }
    }
}
