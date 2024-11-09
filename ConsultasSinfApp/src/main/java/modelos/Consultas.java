package modelos;

import java.util.Calendar;

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
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, -1);
		long fechaMesAtras = calendar.getTimeInMillis();

		String query = "SELECT producto_id, nombre_producto, fecha_compra FROM productos_por_cliente WHERE cliente_id = "
				+ clienteId + " AND fecha_compra>='" + fechaMesAtras + "' ALLOW FILTERING";
		ResultSet resultSet = conexion.getSession().execute(query);

		System.out.println("Productos comprados por el cliente " + clienteId + " en el último mes:");
		for (Row row : resultSet) {
			System.out.println("   Producto ID: " + row.getInt("producto_id") + ", Nombre: "
					+ row.getString("nombre_producto") + ", Fecha: " + row.getTimestamp("fecha_compra"));

		}
	}

	// Consulta 2: Obtener los clientes que compraron un producto específico en el
	// último año.
	public void obtenerClientesQueCompraronProducto(String productoId) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.YEAR, -1);
		long fechaAnioAtras = calendar.getTimeInMillis();

		String query = "SELECT cliente_id, nombre_cliente, fecha_compra FROM clientes_por_producto WHERE producto_id = "
				+ productoId + " AND fecha_compra>='" + fechaAnioAtras + "' ALLOW FILTERING";
		ResultSet resultSet = conexion.getSession().execute(query);

		System.out.println("Clientes que compraron el producto " + productoId + " en el último año:");
		for (Row row : resultSet) {
			System.out.println("   Cliente ID: " + row.getInt("cliente_id") + ", Nombre: "
					+ row.getString("nombre_cliente") + ", Fecha: " + row.getTimestamp("fecha_compra"));
		}

	}

	// Consulta 3: Listar los 10 productos más comprados en una categoría dada.
	public void listarTop10ProductosPorCategoria(String categoria) {
		String query = "SELECT producto_id, nombre_producto, num_compras FROM productos_mas_comprados_por_categoria WHERE categoria = '"
				+ categoria + "' LIMIT 10 ALLOW FILTERING";
		ResultSet resultSet = conexion.getSession().execute(query);

		System.out.println("Top 10 productos más comprados en la categoría " + categoria + ":");
		for (Row row : resultSet) {
			System.out.println("   Producto ID: " + row.getInt("producto_id") + ", Nombre: "
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

	// Consulta 5: Recomendar productos a un cliente basado en otros clientes que
	// compraron productos similares en el último mes.
	public void recomendarProductos(String clienteId) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, -1);
		long fechaMesAtrasDate = calendar.getTimeInMillis();

		String query = "SELECT * from recomendaciones_por_cliente WHERE cliente_id=" + clienteId
				+ " AND fecha_compra>='" + fechaMesAtrasDate + "'";

		ResultSet resultSet = conexion.getSession().execute(query);

		for (Row row : resultSet) {
			System.out.printf(
					"  Cliente ID: %d, Cliente Similar ID: %d, Producto ID: %d, Nombre producto: %s, Fecha: %s\n",
					row.getInt("cliente_id"), row.getInt("cliente_similar_id"), row.getInt("producto_id"),
					row.getString("nombre_producto"), row.getTimestamp("fecha_compra"));
		}
	}
}
