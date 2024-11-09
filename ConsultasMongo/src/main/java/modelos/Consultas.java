package modelos;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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

    // Consulta 1: Listar los clientes que han comprado más de 5 veces en los últimos 6 meses
    public void listarClientesConMasDe5ComprasUltimos6Meses() {
        MongoCollection<Document> collection = database.getCollection("clientes");

        LocalDate fechaLimite = LocalDate.now().minusMonths(6);
        Date fechaLimiteDate = Date.from(fechaLimite.atStartOfDay().toInstant(ZoneOffset.UTC));
        SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                Document cliente = cursor.next();
                List<Document> historialCompras = cliente.getList("historial_compras", Document.class);

                long comprasRecientes = historialCompras.stream()
                        .filter(compra -> {
                            try {
                                Date fechaCompra = formatoFecha.parse(compra.getString("fecha_compra"));
                                return fechaCompra.after(fechaLimiteDate);
                            } catch (ParseException e) {
                                System.err.println("Error al convertir la fecha: " + e.getMessage());
                                return false;
                            }
                        })
                        .count();

                if (comprasRecientes > 5) {
                    Document info = cliente.get("info", Document.class);
                    System.out.printf("ID: %d, Nombre: %s, Edad: %d\n",
                            cliente.getInteger("_id"),
                            info.getString("nombre"),
                            info.getInteger("edad"));
                }
            }
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
                System.out.printf(" Producto: %s, Precio: %.2f, Fecha: %s, Categoría: %s, Calificación: %.1f\n",
                        compra.getString("nombre_producto"), compra.getDouble("precio"),
                        compra.getString("fecha_compra"), compra.getString("categoria"), compra.getDouble("calificacion"));
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



    // Consulta 3: Listar los productos preferidos por los clientes que viven en una ciudad específica
    public void listarProductosPreferidosPorCiudad(String ciudad) {
        MongoCollection<Document> collection = database.getCollection("clientes");

        // Mapa para contar las veces que cada producto aparece como preferido
        Map<String, Integer> contadorProductos = new HashMap<>();

        // Recorrer los clientes en la ciudad especificada
        for (Document cliente : collection.find(new Document("localizacion.ciudad", ciudad))) {
            // Obtener la lista de productos preferidos de cada cliente
            List<String> preferenciasProductos = cliente.getList("preferencias_productos", String.class);

            // Contar cada producto en el mapa
            for (String producto : preferenciasProductos) {
                contadorProductos.put(producto, contadorProductos.getOrDefault(producto, 0) + 1);
            }
        }

        // Ordenar los productos por frecuencia en orden descendente
        List<Map.Entry<String, Integer>> productosOrdenados = contadorProductos.entrySet()
            .stream()
            .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()))
            .collect(Collectors.toList());

        // Mostrar los productos ordenados por frecuencia
        System.out.println("\nProductos preferidos en la ciudad '" + ciudad + "', ordenados por popularidad:");
        for (Map.Entry<String, Integer> entry : productosOrdenados) {
            System.out.println("   Producto: " + entry.getKey() + " - Frecuencia: " + entry.getValue());
        }
    }



    // Consulta 4: Contar cuántos clientes han realizado compras en los últimos 7 días 
    public void contarClientesComprasUltimos7Dias() {
        MongoCollection<Document> collection = database.getCollection("clientes");

        // Calculamos la fecha límite de hace 7 días desde hoy
        LocalDate fechaLimite = LocalDate.now().minusDays(7);
        Date fechaLimiteDate = Date.from(fechaLimite.atStartOfDay().toInstant(ZoneOffset.UTC));
        SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        int contadorClientes = 0;

        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                Document cliente = cursor.next();
                List<Document> historialCompras = cliente.getList("historial_compras", Document.class);

                // Verifica si el cliente tiene al menos una compra en los últimos 7 días
                boolean tieneCompraReciente = historialCompras.stream()
                        .anyMatch(compra -> {
                            try {
                                Date fechaCompra = formatoFecha.parse(compra.getString("fecha_compra"));
                                return fechaCompra.after(fechaLimiteDate);
                            } catch (ParseException e) {
                                System.err.println("Error al convertir la fecha: " + e.getMessage());
                                return false;
                            }
                        });

                if (tieneCompraReciente) {
                    contadorClientes++;
                }
            }
        }

        System.out.println("Clientes con compras en los últimos 7 días: " + contadorClientes);
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
                .forEach(entry -> System.out.println("   Método de pago: " + entry.getKey() + ", Usado: " + entry.getValue() + " veces"));
    }



    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Conexión a MongoDB cerrada.");
        }
    }
}

