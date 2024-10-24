package modelos;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class Consultas {
    private Conexion conexion;

    public Consultas(Conexion conexion) {
        this.conexion = conexion;
    }

    public void listarProductosCompradosPorCliente(String clienteId) {
        String query = "SELECT * FROM compras WHERE cliente_id = ? AND fecha_compra > now() - interval '1' month";
        ResultSet resultSet = conexion.getSession().execute(SimpleStatement.newInstance(query, clienteId));

        System.out.println("Productos comprados por el cliente " + clienteId + " en el último mes:");
        for (Row row : resultSet) {
            System.out.println(row.getString("producto_id") + ": " + row.getDouble("precio"));
        }
    }

    public void obtenerClientesQueCompraronProducto(String productoId) {
        String query = "SELECT DISTINCT cliente_id FROM compras WHERE producto_id = ? AND fecha_compra > now() - interval '1' year";
        ResultSet resultSet = conexion.getSession().execute(SimpleStatement.newInstance(query, productoId));

        System.out.println("Clientes que compraron el producto " + productoId + " en el último año:");
        for (Row row : resultSet) {
            System.out.println(row.getString("cliente_id"));
        }
    }

    public void listarTop10ProductosPorCategoria(String categoria) {
        String query = "SELECT producto_id, COUNT(*) AS total_compras FROM compras WHERE categoria = ? GROUP BY producto_id ORDER BY total_compras DESC LIMIT 10";
        ResultSet resultSet = conexion.getSession().execute(SimpleStatement.newInstance(query, categoria));

        System.out.println("Top 10 productos más comprados en la categoría " + categoria + ":");
        for (Row row : resultSet) {
            System.out.println(row.getString("producto_id") + ": " + row.getInt("total_compras"));
        }
    }

    public void obtenerTotalProductosCompradosPorCliente(String clienteId, String startDate, String endDate) {
        String query = "SELECT COUNT(*) FROM compras WHERE cliente_id = ? AND fecha_compra >= ? AND fecha_compra <= ?";
        ResultSet resultSet = conexion.getSession().execute(SimpleStatement.newInstance(query, clienteId, startDate, endDate));

        System.out.println("Total de productos comprados por el cliente " + clienteId + " entre " + startDate + " y " + endDate + ": " + resultSet.one().getLong(0));
    }

    public void recomendarProductos(String clienteId) {
        // Implementar lógica de recomendación basada en las compras de otros clientes
        // Aquí puedes hacer una consulta más compleja para obtener productos recomendados
        System.out.println("Recomendaciones de productos para el cliente " + clienteId + ":");
        // Simular recomendaciones
        System.out.println("Producto recomendado: Producto X");
    }
}

