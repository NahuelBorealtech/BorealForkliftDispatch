import time
import json
import requests

# Diccionario de mapeo "origen -> destino" según la regla lineal dada por walmart para la POC
DESTINATION_MAP = {
    "AMR01": "AMR08",
    "AMR02": "AMR09",
    "AMR03": "AMR10",
    "AMR04": "AMR11",
    "AMR12": "AMR05",
    "AMR13": "AMR06",
    "AMR14": "AMR07"
}

# Diccionario para registrar los label_ref ya enviados y evitar duplicados.
# Ejemplo: { "00400160200426780644": {"to_slot": "AMR08", "timestamp": ...} }
enviados = {}

def procesar_linea(linea):
    """
    Se espera el formato:
    label_ref timestamp location_id to_slot
    """
    partes = linea.strip().split()
    if len(partes) < 4:
        return None
    return {
        "label_ref": partes[0],
        "timestamp": partes[1],
        "location_id": partes[2],
        "to_slot": partes[3]
    }

def enviar_mision(mision, endpoint="http://localhost:8080/script-api/postOrder"):
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(endpoint, data=json.dumps(mision), headers=headers, timeout=5)
        if response.status_code in (200, 201):
            return True
        else:
            print("Error en envío:", response.status_code, response.text)
            return False
    except Exception as e:
        print("Excepción al enviar misión:", e)
        return False

def leer_archivo(filename="labels_disp.txt"):
    try:
        with open(filename, 'r') as f:
            return f.readlines()
    except Exception as e:
        print("Error al leer el archivo:", e)
        return []

def escribir_status(label_ref, from_slot, to_slot, status, up_ts, filename="AMR_STATUS.txt"):
    """
    Escribe una línea en AMR_STATUS.txt con el formato:
    label_ref from_slot to_slot AMR_estatus up_ts
    """
    try:
        with open(filename, 'a') as f:
            # Ajusta los espacios o relleno según necesites, aquí es un ejemplo simple.
            linea = f"{label_ref} {from_slot} {to_slot} {status} {up_ts}\n"
            f.write(linea)
    except Exception as e:
        print("Error al escribir AMR_STATUS.txt:", e)

if __name__ == "__main__":
    while True:
        lineas = leer_archivo()
        for linea in lineas:
            dato = procesar_linea(linea)
            if not dato:
                continue

            label = dato["label_ref"]
            # Evitar duplicados.
            if label in enviados:
                continue

            from_slot = dato["to_slot"]  # El slot de origen es el que viene en el txt de input

            # Determinar el destino según el mapeo.
            if from_slot not in DESTINATION_MAP:
                print(f"No hay destino definido para {from_slot}, se ignora la línea.")
                continue
            final_slot = DESTINATION_MAP[from_slot]

            # Construir la misión a enviar.
            mision = {
                "label_ref": label,
                "to_slot": from_slot,     # Origen
                "final_slot": final_slot  # Destino
            }

            print("Intentando enviar misión:", mision)
            if enviar_mision(mision):
                # Registrar en 'enviados' para evitar reenvío.
                enviados[label] = {"to_slot": final_slot, "timestamp": dato["timestamp"]}
                print("Misión enviada y confirmada para", label)

                # Escribir en AMR_STATUS.txt
                escribir_status(
                    label_ref=label,
                    from_slot=from_slot,
                    to_slot=final_slot,
                    status="TAREA*ENVIADA*******",  
                    up_ts=dato["timestamp"]
                )
            else:
                print("Fallo al enviar la misión para", label)

        # Esperar unos segundos antes de volver a leer el txt de imput
        time.sleep(5)
