from faker import Faker

# Inicializar Faker
faker = Faker()

# Abrir el archivo para guardar los nombres
with open('nombres.txt', 'w') as file:
    # Generar y guardar nombres
    for _ in range(5000):  # Cambia el rango si quieres más o menos nombres
        nombre_cliente = faker.name()  # Generar un nombre aleatorio
        file.write(nombre_cliente + '\n')  # Escribir en el archivo

print("Nombres generados y guardados en 'nombres.txt'.")
