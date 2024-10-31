package modelos;

import java.util.Date;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Consultas {
	private Conexion conexion;

	public Consultas(Conexion conexion) {
		this.conexion = conexion;
	}

	// Consulta 1: Listar los productos comprados por un cliente (ID de cliente) en
	// el último mes.
	public void listarProductosCompradosPorCliente(String clienteId) {
		String query = "SELECT producto_id, nombre_producto, fecha_compra FROM productos_por_cliente WHERE cliente_id = "
				+ clienteId+" ALLOW FILTERING";
		ResultSet resultSet = conexion.getSession().execute(query);

		long dias30EnMillis = 30L * 24 * 60 * 60 * 1000; // 30 días en milisegundos
		long ahoraEnMillis = System.currentTimeMillis();

		System.out.println("Productos comprados por el cliente " + clienteId + " en el último mes:");
		for (Row row : resultSet) {
			Date fechaCompra = row.getTimestamp("fecha_compra");
			if (ahoraEnMillis - fechaCompra.getTime() <= dias30EnMillis) {
				System.out.println("Producto ID: " + row.getInt("producto_id") + ", Nombre: "
						+ row.getString("nombre_producto") + ", Fecha: " + fechaCompra);
			}
		}
	}

	// Consulta 2: Obtener los clientes que compraron un producto específico en el
	// último año.
	public void obtenerClientesQueCompraronProducto(String productoId) {
	}

	// Consulta 3: Listar los 10 productos más comprados en una categoría dada.
	public void listarTop10ProductosPorCategoria(String categoria) {
		String query = "SELECT producto_id, nombre_producto, num_compras FROM productos_mas_comprados_por_categoria WHERE categoria = '"
				+ categoria + "' LIMIT 10 ALLOW FILTERING";
		ResultSet resultSet = conexion.getSession().execute(query);

		System.out.println("Top 10 productos más comprados en la categoría " + categoria + ":");
		for (Row row : resultSet) {
			System.out.println("Producto ID: " + row.getInt("producto_id") + ", Nombre: "
					+ row.getString("nombre_producto") + ", Compras: " + row.getLong("num_compras"));
		}
	}

	// Consulta 4: Obtener el total de productos comprados por un cliente en un
	// periodo de tiempo.
	public void obtenerTotalProductosCompradosPorCliente(String clienteId, String fechaInicio, String fechaFin) {
		String query = "SELECT COUNT(*) FROM productos_por_cliente WHERE cliente_id = " + clienteId
				+ " AND fecha_compra >= '" + fechaInicio + "' AND fecha_compra <= '" + fechaFin + "'";

		ResultSet resultSet = conexion.getSession().execute(query);
		long totalCompras = resultSet.one().getLong(0);

		System.out.println("El total de productos comprados por el cliente " + clienteId + " entre " + fechaInicio
				+ " y " + fechaFin + ": " + totalCompras);
	}

	public void recomendarProductos(String clienteId) {
	}
}
