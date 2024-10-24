package modelos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

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

    // Consulta 1: Listar los clientes que han comprado más de 5 veces en los últimos 6 meses
    public void listarClientesConMasDe5ComprasUltimos6Meses() {
    	MongoCollection<Document> collection = database.getCollection("clientes");

        // Obtener la fecha actual y calcular la fecha límite (6 meses atrás)
        Date ahora = new Date();
        Date seisMesesAtras = new Date(ahora.getTime() - (180L * 24 * 60 * 60 * 1000)); // 6 meses atrás

        // Filtrar los documentos
        List<Document> clientes = collection.aggregate(Arrays.asList(
            // Agrupar por cliente y contar compras
            Aggregates.match(Filters.gt("historial_compras.fecha_compra", seisMesesAtras)),
            Aggregates.group("$cliente_id", Accumulators.sum("totalCompras", 1)),
            Aggregates.match(Filters.gt("totalCompras", 5))
        )).into(new ArrayList<>());

        // Mostrar los resultados
        if (clientes.isEmpty()) {
            System.out.println("No se encontraron clientes que hayan comprado más de 5 veces en los últimos 6 meses.");
        } else {
            System.out.println("Clientes que han comprado más de 5 veces en los últimos 6 meses:");
            for (Document cliente : clientes) {
                System.out.println(cliente.toJson());
            }
        }
    }

    // Consulta 2: Obtener el perfil de un cliente dado su ID
    public void obtenerPerfilCliente(String clienteId) {
        MongoCollection<Document> collection = database.getCollection("clientes");
        //ObjectId objectId=new ObjectId(clienteId);
        Document cliente = collection.find(new Document("_id", Integer.parseInt(clienteId))).first();

        if (cliente != null) {
            System.out.println("Perfil del cliente: " + cliente.toJson());
        } else {
            System.out.println("Cliente no encontrado.");
        }
    }

    // Consulta 3: Listar los productos preferidos por los clientes que viven en una ciudad específica
    public void listarProductosPreferidosPorCiudad(String ciudad) {
        MongoCollection<Document> collection = database.getCollection("clientes");

        for (Document cliente : collection.find(new Document("localizacion.ciudad", ciudad))) {
            // Acceder al campo 'info' y luego al 'nombre'
            Document info = cliente.get("info", Document.class); // Obtener el subdocumento 'info'
            String nombre = info.getString("nombre");

            System.out.println("Cliente: " + nombre);
            System.out.println("Productos preferidos: " + cliente.getList("preferencias_productos", String.class));
        }
    }


    // Consulta 4: Contar cuántos clientes han realizado compras en los últimos 7 días
    public void contarClientesComprasUltimos7Dias() {
        MongoCollection<Document> collection = database.getCollection("clientes");
        Date fechaLimite = new Date(System.currentTimeMillis() - (7L * 24 * 60 * 60 * 1000)); // Últimos 7 días

        long clientesConComprasRecientes = collection.countDocuments(new Document("historial_compras.fecha_compra",
                new Document("$gte", fechaLimite)));

        System.out.println("Clientes con compras en los últimos 7 días: " + clientesConComprasRecientes);
    }

    // Consulta 5: Listar los métodos de pago más utilizados por los clientes que han hecho más de 10 compras
    public void listarMetodosPagoMasUsados() {
        MongoCollection<Document> collection = database.getCollection("clientes");
        Map<String, Integer> metodosPagoFrecuencia = new HashMap<>();

        for (Document cliente : collection.find()) {
            long numCompras = cliente.getList("historial_compras", Document.class).size();

            if (numCompras > 10) {
                List<String> metodosPago = cliente.getList("metodos_pago", String.class);

                for (String metodo : metodosPago) {
                    metodosPagoFrecuencia.put(metodo, metodosPagoFrecuencia.getOrDefault(metodo, 0) + 1);
                }
            }
        }

        // Ordenar los métodos de pago por frecuencia
        metodosPagoFrecuencia.entrySet().stream()
                .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()))
                .forEach(entry -> System.out.println("Método de pago: " + entry.getKey() + ", Usado: " + entry.getValue() + " veces"));
    }



    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Conexión a MongoDB cerrada.");
        }
    }
}

