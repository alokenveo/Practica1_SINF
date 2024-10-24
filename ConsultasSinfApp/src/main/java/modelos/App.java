package modelos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {
    public static void main(String[] args) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Conexion conexion = new Conexion("127.0.0.1", 9042); // Cambia la dirección y puerto según tu configuración
        Consultas consultas = new Consultas(conexion);

        while (true) {
            mostrarMenu();
            try {
                System.out.print("Selecciona una consulta: ");
                int seleccion = Integer.parseInt(reader.readLine());

                switch (seleccion) {
                    case 1:
                        System.out.print("Introduce el ID del cliente: ");
                        String clienteId = reader.readLine();
                        consultas.listarProductosCompradosPorCliente(clienteId);
                        break;
                    case 2:
                        System.out.print("Introduce el ID del producto: ");
                        String productoId = reader.readLine();
                        consultas.obtenerClientesQueCompraronProducto(productoId);
                        break;
                    case 3:
                        System.out.print("Introduce la categoría: ");
                        String categoria = reader.readLine();
                        consultas.listarTop10ProductosPorCategoria(categoria);
                        break;
                    case 4:
                        System.out.print("Introduce el ID del cliente: ");
                        String idCliente = reader.readLine();
                        System.out.print("Introduce la fecha de inicio (YYYY-MM-DD): ");
                        String startDate = reader.readLine();
                        System.out.print("Introduce la fecha de fin (YYYY-MM-DD): ");
                        String endDate = reader.readLine();
                        consultas.obtenerTotalProductosCompradosPorCliente(idCliente, startDate, endDate);
                        break;
                    case 5:
                        System.out.print("Introduce el ID del cliente: ");
                        String cliente = reader.readLine();
                        consultas.recomendarProductos(cliente);
                        break;
                    case 6:
                        conexion.cerrarConexion();
                        System.out.println("Saliendo del programa.");
                        return;
                    default:
                        System.out.println("Opción no válida. Por favor, intenta de nuevo.");
                }
            } catch (IOException e) {
                System.out.println("Error al leer la entrada: " + e.getMessage());
            } catch (NumberFormatException e) {
                System.out.println("Por favor, introduce un número válido.");
            }
        }
    }

    private static void mostrarMenu() {
        System.out.println("Consultas disponibles:");
        System.out.println("  Consulta 1: Listar los productos comprados por un cliente (ID de cliente) en el último mes.");
        System.out.println("  Consulta 2: Obtener los clientes que compraron un producto específico en el último año.");
        System.out.println("  Consulta 3: Listar los 10 productos más comprados en una categoría dada.");
        System.out.println("  Consulta 4: Obtener el total de productos comprados por un cliente en un periodo de tiempo.");
        System.out.println("  Consulta 5: Recomendar productos a un cliente basado en otros clientes que compraron productos similares en el último mes.");
        System.out.println("  Consulta 6: Salir del programa.");
    }
}

