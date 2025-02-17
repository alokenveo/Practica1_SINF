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

	public void obtenerTotalProductosCompradosPorCliente(String clienteId, String fechaInicio, String fechaFin) {
		// Consulta para obtener los productos comprados por el cliente en el rango de
		// fechas
		String query = "SELECT producto_id, nombre_producto FROM productos_por_cliente " + "WHERE cliente_id = "
				+ clienteId + " AND fecha_compra >= '" + fechaInicio + "' AND fecha_compra <= '" + fechaFin + "'";

		ResultSet resultSet = conexion.getSession().execute(query);

		// Obtenemos el total de filas devueltas
		int totalCompras = resultSet.getAvailableWithoutFetching(); // Este método te da el número de filas sin tener
																	// que recorrer todo el conjunto de resultados

		// Mostrar el total de productos comprados
		System.out.println("\nEl total de productos comprados por el cliente " + clienteId + " entre " + fechaInicio
				+ " y " + fechaFin + ": " + totalCompras);

		// Recorremos las filas devueltas para mostrar los detalles de los productos
		System.out.println("Detalles de los productos comprados:");
		for (Row row : resultSet) {
			System.out.printf("   Producto ID: %d, Nombre: %s\n", row.getInt("producto_id"),
					row.getString("nombre_producto"));
		}
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

	// DEFENSA
	// Consulta 6: Lista los productos más caros por categoría
	public void listarProductosMasCarosPorCategoria(String categoria) {
		String query = "SELECT producto_id, nombre_producto, precio FROM productos_mas_caros_por_categoria WHERE categoria = '"
				+ categoria + "' LIMIT 10";

		ResultSet resultSet = conexion.getSession().execute(query);

		System.out.println("Productos más caros en la categoría " + categoria + ":");
		for (Row row : resultSet) {
			System.out.printf("   Producto ID: %d, Nombre: %s, Precio: %.2f\n", row.getInt("producto_id"),
					row.getString("nombre_producto"), row.getDecimal("precio"));
		}
	}

}
