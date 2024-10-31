package modelos;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class Conexion {
	private Cluster cluster;
	private Session session;

	public Conexion() {
	}

	@SuppressWarnings("deprecation")
	public void connect(String node, String keyspace) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}

		// Conectar al keyspace
		session = cluster.connect(keyspace);
		System.out.printf("Connected to keyspace: %s\n", keyspace);
	}

	public Session getSession() {
		return session;
	}

	public void cerrarConexion() {
		if (session != null) {
			session.close();
			cluster.close();
		}
	}
}
