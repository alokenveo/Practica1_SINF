import time

def contador():
    for i in range(1, 101):
        # Imprimir el conteo en la misma línea
        print(f"\rContador: {i}", end='')
        time.sleep(0.1)  # Pausa de 0.1 segundos

    print()  # Imprimir una nueva línea al finalizar

contador()


