import time
import json
import requests
import threading

# ------------------ Configuración y Funciones para el envío de misiones ------------------

# Diccionario de mapeo "origen -> destino" según la regla lineal dada.
# Ejemplo: AMR01 -> AMR08, AMR02 -> AMR09, etc.
DESTINATION_MAP = {
    "AMR01": "AMR08",
    "AMR02": "AMR09",
    "AMR03": "AMR10",
    "AMR04": "AMR11",
    "AMR12": "AMR05",
    "AMR13": "AMR06",
    "AMR14": "AMR07"
}

# Diccionario para registrar las combinaciones (label_ref, from_slot) ya enviadas.
# Clave: (label_ref, from_slot)
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

def escribir_status(label_ref, from_slot, to_slot, status, up_ts, down_ts="", filename="AMR_STATUS.txt"):
    """
    Escribe una línea en AMR_STATUS.txt con el formato:
    label_ref from_slot to_slot AMR_estatus up_ts [down_ts]
    Si down_ts se suministra (en estado FINALIZADA), se incluye.
    """
    try:
        with open(filename, 'a') as f:
            if down_ts:
                linea = f"{label_ref} {from_slot} {to_slot} {status} {up_ts} {down_ts}\n"
            else:
                linea = f"{label_ref} {from_slot} {to_slot} {status} {up_ts}\n"
            f.write(linea)
    except Exception as e:
        print("Error al escribir AMR_STATUS.txt:", e)

def sender_loop():
    """Hilo que lee labels_disp.txt y envía nuevas misiones a RDS."""
    while True:
        lineas = leer_archivo()
        for linea in lineas:
            dato = procesar_linea(linea)
            if not dato:
                continue

            label = dato["label_ref"]
            from_slot = dato["to_slot"]  # El slot de origen según el txt

            # La clave es la combinación (label_ref, from_slot)
            clave = (label, from_slot)
            if clave in enviados:
                continue  # Ya se envió esta combinación

            if from_slot not in DESTINATION_MAP:
                print(f"No hay destino definido para {from_slot}, se ignora la línea.")
                continue

            final_slot = DESTINATION_MAP[from_slot]

            # Construir la misión
            mision = {
                "label_ref": label,
                "to_slot": from_slot,     # Origen
                "final_slot": final_slot  # Destino
            }

            print("Intentando enviar misión:", mision)
            if enviar_mision(mision):
                enviados[clave] = {"to_slot": final_slot, "timestamp": dato["timestamp"]}
                print("Misión enviada y confirmada para", label)
                # Registrar en AMR_STATUS.txt con estado inicial "TAREA*ENVIADA*******"
                escribir_status(
                    label_ref=label,
                    from_slot=from_slot,
                    to_slot=final_slot,
                    status="TAREA*ENVIADA*******",
                    up_ts=dato["timestamp"]
                )
            else:
                print("Fallo al enviar la misión para", label)
        time.sleep(5)

# ------------------ Funciones para monitorear y actualizar el estado de las misiones ------------------

def update_statuses():
    filename = "AMR_STATUS.txt"
    try:
        with open(filename, 'r') as f:
            lines = f.readlines()
    except Exception as e:
        print("Error al leer AMR_STATUS.txt:", e)
        return
    
    updated_lines = []
    file_changed = False

    for line in lines:
        parts = line.strip().split()
        # Se esperan al menos 5 campos: label_ref, from_slot, to_slot, status, up_ts
        if len(parts) < 5:
            updated_lines.append(line)
            continue

        label_ref, from_slot, to_slot, status, up_ts = parts[:5]
        down_ts = parts[5] if len(parts) >= 6 else ""

        # Si ya está finalizada, se mantiene.
        if status == "FINALIZADA**********":
            updated_lines.append(line)
            continue

        # Construir taskRecordId como: from_slot + label_ref
        taskRecordId = from_slot + label_ref
        print(f"Consultando taskRecordId: {taskRecordId}")  # Depuración

        url = "http://localhost:8080/api/queryBlocksByTaskId"
        headers = {"language": "en", "Content-Type": "application/json"}
        payload = {"taskRecordId": taskRecordId}
        try:
            r = requests.post(url, data=json.dumps(payload), headers=headers, timeout=5)
            if r.status_code not in (200, 201):
                print("Error en consulta de task:", r.status_code, r.text)
                updated_lines.append(line)
                continue
            resp = r.json()
            print("Respuesta JSON:", resp)  # Depuración
        except Exception as e:
            print("Excepción al consultar taskRecordId", taskRecordId, ":", e)
            updated_lines.append(line)
            continue

        data = resp.get("data", {})
        blockList = data.get("blockList", [])
        # Filtrar bloques cuyo blockLabel sea "Robot Dispatch"
        robot_dispatch_blocks = [b for b in blockList if b.get("blockLabel", "").strip().lower() == "robot dispatch"]
        print(f"Bloques robot dispatch encontrados: {len(robot_dispatch_blocks)}")  # Depuración

        if not robot_dispatch_blocks:
            updated_lines.append(line)
            continue

        # Nueva lógica: contar cuántos bloques están en estado 1003.
        total = len(robot_dispatch_blocks)
        complete_count = sum(1 for b in robot_dispatch_blocks if b.get("status") == 1003)
        print(f"Bloques completos: {complete_count} de {total}")  # Depuración

        new_status = status
        new_down_ts = down_ts

        if complete_count > 0 and complete_count < total:
            new_status = "PALLET*EN*CAMINO****"
        elif total > 0 and complete_count == total:
            new_status = "FINALIZADA**********"
            new_down_ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

        if new_status != status or (new_status == "FINALIZADA**********" and new_down_ts != down_ts):
            file_changed = True
            if new_status == "FINALIZADA**********":
                new_line = f"{label_ref} {from_slot} {to_slot} {new_status} {up_ts} {new_down_ts}\n"
            else:
                new_line = f"{label_ref} {from_slot} {to_slot} {new_status} {up_ts}\n"
            print(f"Actualizando estado para {label_ref} (origen {from_slot}): {status} -> {new_status}")
            updated_lines.append(new_line)
        else:
            updated_lines.append(line)

    if file_changed:
        try:
            with open(filename, 'w') as f:
                f.writelines(updated_lines)
        except Exception as e:
            print("Error al escribir AMR_STATUS.txt:", e)


def monitor_status_loop():
    """Hilo que revisa periódicamente el estado de las misiones en AMR_STATUS.txt."""
    while True:
        update_statuses()
        time.sleep(5)

# ------------------ Inicio de los hilos ------------------

if __name__ == "__main__":
    sender_thread = threading.Thread(target=sender_loop, daemon=True)
    monitor_thread = threading.Thread(target=monitor_status_loop, daemon=True)
    
    sender_thread.start()
    monitor_thread.start()
    
    # Mantener el proceso corriendo
    while True:
        time.sleep(1)
