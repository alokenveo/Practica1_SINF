package modelos;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Consultas {

	private MongoClient mongoClient;
	private MongoDatabase database;

	public Consultas() {
		connectToMongoDB();
	}

	private void connectToMongoDB() {
		try {
			mongoClient = MongoClients.create("mongodb://localhost:27017");
			database = mongoClient.getDatabase("marketing");
			System.out.println("Conectado a MongoDB");
		} catch (Exception e) {
			System.err.println("Error al conectar a MongoDB: " + e.getMessage());
		}
	}

	// Consulta 1: Listar los clientes que han comprado más de 5 veces en los
	// últimos 6 meses
	public void listarClientesConMasDe5ComprasUltimos6Meses() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, -6);
		Date fechaCorte = calendar.getTime(); // Esto convierte a tipo Date

		MongoCollection<Document> collection = database.getCollection("clientes");

		// Crear la consulta de agregación
		AggregateIterable<Document> result = collection.aggregate(Arrays.asList(
				new Document("$project", new Document("nombre", "$info.nombre").append("historial_compras", 1)),
				new Document("$unwind", "$historial_compras"),
				new Document("$match",
						new Document("historial_compras.fecha_compra", new Document("$gte", fechaCorte))),
				new Document("$group",
						new Document("_id", "$_id").append("nombre", new Document("$first", "$nombre"))
								.append("total_compras", new Document("$sum", 1))),
				new Document("$match", new Document("total_compras", new Document("$gt", 5))),
				new Document("$sort", new Document("_id", 1))));

		// Mostrar los resultados
		for (Document doc : result) {
			System.out.println("   ID: " + doc.get("_id") + "; Nombre: " + doc.get("nombre"));
		}
	}

	// Consulta 2: Obtener el perfil de un cliente dado su ID
	public void obtenerPerfilCliente(String clienteId) {
		MongoCollection<Document> collection = database.getCollection("clientes");
		Document cliente = collection.find(new Document("_id", Integer.parseInt(clienteId))).first();

		if (cliente != null) {
			Document info = cliente.get("info", Document.class);
			List<Document> historialCompras = cliente.getList("historial_compras", Document.class);
			List<String> preferenciasProductos = cliente.getList("preferencias_productos", String.class);
			List<Document> historialInteracciones = cliente.getList("historial_interacciones_web", Document.class);
			Document localizacion = cliente.get("localizacion", Document.class);
			List<String> metodosPago = cliente.getList("metodos_pago", String.class);

			System.out.println("Perfil del Cliente:");
			System.out.println(" ID: " + cliente.getInteger("_id"));
			System.out.println(" Nombre: " + info.getString("nombre"));
			System.out.println(" Correo: " + info.getString("correo"));
			System.out.println(" Edad: " + info.getInteger("edad"));

			System.out.println("\nHistorial de Compras:");
			for (Document compra : historialCompras) {
				// Obtén la fecha de la compra como Date y luego convierte a String si es
				// necesario
				Date fechaCompra = compra.getDate("fecha_compra");
				String fechaCompraStr = fechaCompra != null ? fechaCompra.toString() : "Fecha no disponible";

				System.out.printf(" Producto: %s, Precio: %.2f, Fecha: %s, Categoría: %s, Calificación: %.1f\n",
						compra.getString("nombre_producto"), compra.getDouble("precio"), fechaCompraStr,
						compra.getString("categoria"), compra.getDouble("calificacion"));
			}

			System.out.println("\nPreferencias de Productos: " + String.join(", ", preferenciasProductos));

			if (historialInteracciones != null && !historialInteracciones.isEmpty()) {
				System.out.println("\nHistorial de Interacciones en la Web:");
				for (Document interaccion : historialInteracciones) {
					System.out.println(" Última visita: " + interaccion.getString("ultima_visita"));
					System.out.println(" Búsquedas: " + interaccion.getList("busquedas", String.class));
					System.out.println(" Clics: " + interaccion.getInteger("clics"));
				}
			}

			System.out.println("\nLocalización:");
			System.out.println(" Ciudad: " + localizacion.getString("ciudad"));
			System.out.println(" País: " + localizacion.getString("pais"));

			System.out.println("\nMétodos de Pago: " + String.join(", ", metodosPago));
		} else {
			System.out.println("Cliente no encontrado.");
		}
	}

	// Consulta 3: Listar los productos preferidos por los clientes que viven en una
	// ciudad específica
	public void listarProductosPreferidosPorCiudad(String ciudad) {
		MongoCollection<Document> collection = database.getCollection("clientes");

		// Definir las etapas de la agregación
		Document match = new Document("$match", new Document("localizacion.ciudad", ciudad));
		Document unwind = new Document("$unwind", "$preferencias_productos");
		Document group = new Document("$group",
				new Document("_id", "$preferencias_productos").append("frecuencia", new Document("$sum", 1)));
		Document sort = new Document("$sort", new Document("frecuencia", -1));

		// Ejecutar la agregación
		AggregateIterable<Document> resultado = collection.aggregate(Arrays.asList(match, unwind, group, sort));

		// Imprimir el resultado
		for (Document doc : resultado) {
			System.out
					.println("   Producto: " + doc.getString("_id") + ", Frecuencia: " + doc.getInteger("frecuencia"));
		}

	}

	// Consulta 4: Contar cuántos clientes han realizado compras en los últimos 7
	// días
	public void contarClientesComprasUltimos7Dias() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -7);
		Date fechaCorte = calendar.getTime();

		MongoCollection<Document> collection = database.getCollection("clientes");

		Document filtro = new Document("historial_compras.fecha_compra", new Document("$gte", fechaCorte));
		long conteo = collection.countDocuments(filtro);

		System.out.println("Clientes con compras en los últimos 7 días(" + fechaCorte + "): " + conteo);

	}

	// Consulta 5: Listar los métodos de pago más utilizados por los clientes que
	// han hecho más de 10 compras
	public void listarMetodosPagoMasUsados() {
		MongoCollection<Document> collection = database.getCollection("clientes");

		// Definir las etapas de la agregación
		Document project = new Document("$project",
				new Document("metodos_pago", 1).append("total_compras", new Document("$size", "$historial_compras")));

		Document match = new Document("$match", new Document("total_compras", new Document("$gt", 10)));
		Document unwind = new Document("$unwind", "$metodos_pago");
		Document group = new Document("$group",
				new Document("_id", "$metodos_pago").append("totalClientes", new Document("$sum", 1)));
		Document sort = new Document("$sort", new Document("totalClientes", -1));

		// Ejecutar la agregación
		AggregateIterable<Document> resultado = collection
				.aggregate(Arrays.asList(project, match, unwind, group, sort));

		// Imprimir el resultado
		for (Document doc : resultado) {
			System.out.println("   Método de pago: " + doc.getString("_id") + ", Usado: "
					+ doc.getInteger("totalClientes") + " veces");
		}
	}

	public void close() {
		if (mongoClient != null) {
			mongoClient.close();
			System.out.println("Conexión a MongoDB cerrada.");
		}
	}
}
