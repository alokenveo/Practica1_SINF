package modelos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class App {

	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		Consultas consultas = new Consultas();
		

		while (true) {
			mostrarMenu();
			System.out.print("Selecciona una consulta: ");
			int opcion = Integer.parseInt(reader.readLine());

			switch (opcion) {
			case 1:
				consultas.listarClientesConMasDe5ComprasUltimos6Meses();
				break;
			case 2:
				System.out.print("Ingresa el ID del cliente: ");
				String clienteId = reader.readLine();
				consultas.obtenerPerfilCliente(clienteId);
				break;
			case 3:
				System.out.print("Ingresa la ciudad: ");
				String ciudad = reader.readLine();
				consultas.listarProductosPreferidosPorCiudad(ciudad);
				break;
			case 4:
				consultas.contarClientesComprasUltimos7Dias();
				break;
			case 5:
				consultas.listarMetodosPagoMasUsados();
				break;
			case 6:
				System.out.println("Saliendo del programa... Gracias por todo :)");
				return;
			default:
				System.out.println("Opción no válida.");
			}

		}
	}

	private static void mostrarMenu() {
		System.out.println("\nConsultas disponibles:");
		System.out.println("  Consulta 1: Listar los clientes que han comprado más de 5 veces en los últimos 6 meses.");
		System.out.println("  Consulta 2: Obtener el perfil de un cliente dado su ID.");
		System.out.println(
				"  Consulta 3: Listar los productos preferidos por los clientes que viven en una ciudad específica.");
		System.out.println("  Consulta 4: Contar cuántos clientes han realizado compras en los últimos 7 días.");
		System.out.println(
				"  Consulta 5: Listar los métodos de pago más utilizados por los clientes que han hecho más de 10 compras.");
		System.out.println("  Consulta 6: Salir del programa.");
	}
}
