import time
import json
import requests
import threading
import sys
from datetime import datetime

# ------------------ Variables Globales para Control de Alternancia y Buffer ------------------

# Control para enviar sólo una misión a la vez
mission_in_progress = False  
# Almacena la última fase procesada (1 o 2). Si es None, significa que aún no se ha procesado ninguna misión.
last_mission_phase = None  
# Tiempo (en segundos, timestamp) cuando la última misión finalizó
last_mission_end_time = 0  

# ------------------ Configuración y Mapeo de Destinos ------------------

# Diccionario de mapeo "origen -> destino" según la regla lineal dada.
DESTINATION_MAP = {
    "AMR01": "AMR08",
    "AMR02": "AMR09",
    "AMR03": "AMR10",
    "AMR04": "AMR11",
    "AMR12": "AMR05",
    "AMR13": "AMR06",
    "AMR14": "AMR07"
}

# Diccionario para registrar las misiones ya enviadas.
# Clave: (label_ref, from_slot)
enviados = {}

# ------------------ Funciones de Utilidad ------------------

def determine_phase(from_slot):
    """
    Determina la fase según el slot de origen:
      - Fase 1: AMR01, AMR02, AMR03, AMR04
      - Fase 2: AMR12, AMR13, AMR14
    """
    if from_slot in ["AMR01", "AMR02", "AMR03", "AMR04"]:
        return 1
    elif from_slot in ["AMR12", "AMR13", "AMR14"]:
        return 2
    else:
        return None

def is_highest_priority(candidato, pendientes):
    """
    Verifica que, para el buffer correspondiente, no exista ya en espera (pendientes)
    una misión de mayor prioridad.
    
    Para Buffer 1 (salida): prioridad: AMR01 > AMR02 > AMR03 > AMR04  
    Para Buffer 2 (salida): prioridad: AMR12 > AMR13 > AMR14  
    """
    slot = candidato["to_slot"]
    phase = determine_phase(slot)
    if phase == 1:
        prioridad = ["AMR01", "AMR02", "AMR03", "AMR04"]
    elif phase == 2:
        prioridad = ["AMR12", "AMR13", "AMR14"]
    else:
        return False  # No se reconoce el buffer, no se procesa
    
    indice_candidato = prioridad.index(slot)
    # Iteramos sobre los pendientes (excluyendo los que ya han sido enviados)
    for p in pendientes:
        # Sólo analizar misiones del mismo buffer
        if determine_phase(p["to_slot"]) != phase:
            continue
        # Si existe una misión con un slot de mayor prioridad (índice menor)
        if prioridad.index(p["to_slot"]) < indice_candidato:
            return False
    return True

def procesar_linea(linea):
    """
    Procesa la línea del archivo y retorna un diccionario con:
    label_ref, timestamp, location_id, to_slot
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

def log_error(message):
    """Registra errores en un archivo de log"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("error_log.txt", "a") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        print(f"Error al escribir en el log: {e}")

# ------------------ Hilo de Envío con Lógica de Buffer y Alternancia de Fases ------------------

def sender_loop():
    global mission_in_progress, last_mission_phase, last_mission_end_time
    while getattr(threading.current_thread(), "do_run", True):
        try:
            # Si ya hay una misión en curso, no se envía otra.
            if mission_in_progress:
                time.sleep(2)
                continue

            lineas = leer_archivo()
            pendientes = []
            # Construir la lista de misiones pendientes (no enviadas)
            for linea in lineas:
                dato = procesar_linea(linea)
                if not dato:
                    continue
                clave = (dato["label_ref"], dato["to_slot"])
                if clave in enviados:
                    continue
                # Sólo se consideran aquellas misiones cuyo from_slot esté definido en el mapa
                if dato["to_slot"] not in DESTINATION_MAP:
                    log_error(f"No hay destino definido para {dato['to_slot']}, se ignora la línea.")
                    continue
                pendientes.append(dato)
            
            if not pendientes:
                time.sleep(2)
                continue

            # Filtrar candidatos que cumplan con la prioridad en su buffer
            candidatos = [p for p in pendientes if is_highest_priority(p, pendientes)]
            if not candidatos:
                # No hay candidatos listos por orden en buffer
                time.sleep(2)
                continue

            # Clasificar candidatos por fase:
            candidatos_fase1 = [p for p in candidatos if determine_phase(p["to_slot"]) == 1]
            candidatos_fase2 = [p for p in candidatos if determine_phase(p["to_slot"]) == 2]

            # Determinar la fase requerida:
            current_time = time.time()
            # Para la primera misión, se da prioridad a FASE 1 si hay candidatos
            if last_mission_phase is None:
                required_phase = 1
            else:
                required_phase = 2 if last_mission_phase == 1 else 1

            candidatos_requeridos = candidatos_fase1 if required_phase == 1 else candidatos_fase2

            # Si no hay candidatos en la fase requerida, pero han pasado 2 minutos desde la última misión,
            # se permiten misiones de la otra fase.
            if not candidatos_requeridos:
                if last_mission_phase is not None and (current_time - last_mission_end_time) >= 120:
                    # Se permite elegir de cualquiera
                    candidatos_requeridos = candidatos
                else:
                    # No hay candidatos válidos por alternancia
                    time.sleep(2)
                    continue

            # Se puede ordenar por timestamp o por prioridad de slot (ya comprobado en is_highest_priority).
            # Aquí se elige la misión con el menor timestamp (más antigua) de los candidatos_requeridos.
            candidato = sorted(candidatos_requeridos, key=lambda x: x["timestamp"])[0]
            label = candidato["label_ref"]
            from_slot = candidato["to_slot"]
            final_slot = DESTINATION_MAP[from_slot]
            mision = {
                "label_ref": label,
                "to_slot": from_slot,
                "final_slot": final_slot
            }

            print("Intentando enviar misión:", mision)
            if enviar_mision(mision):
                clave = (label, from_slot)
                enviados[clave] = {"to_slot": final_slot, "timestamp": candidato["timestamp"]}
                mission_in_progress = True  # Marcamos que hay una misión en curso
                print("Misión enviada y confirmada para", label)
                escribir_status(
                    label_ref=label,
                    from_slot=from_slot,
                    to_slot=final_slot,
                    status="TAREA*ENVIADA*******",
                    up_ts=candidato["timestamp"]
                )
            else:
                log_error(f"Fallo al enviar la misión para {label}")
            
        except Exception as e:
            log_error(f"Error en sender_loop: {str(e)}")
        time.sleep(5)

# ------------------ Monitoreo y Actualización del Estado de las Misiones ------------------

def update_statuses():
    global mission_in_progress, last_mission_phase, last_mission_end_time
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

        # Construir taskRecordId (esta parte permanece igual para la consulta)
        taskRecordId = from_slot + label_ref
        print(f"Consultando taskRecordId: {taskRecordId}")
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
            print("Respuesta JSON:", resp)
        except Exception as e:
            print("Excepción al consultar taskRecordId", taskRecordId, ":", e)
            updated_lines.append(line)
            continue

        data = resp.get("data", {})
        blockList = data.get("blockList", [])
        # Filtrar bloques cuyo blockLabel sea "Robot Dispatch"
        robot_dispatch_blocks = [b for b in blockList if b.get("blockLabel", "").strip().lower() == "robot dispatch"]
        print(f"Bloques robot dispatch encontrados: {len(robot_dispatch_blocks)}")

        total = len(robot_dispatch_blocks)
        complete_count = sum(1 for b in robot_dispatch_blocks if b.get("status") == 1003)
        print(f"Bloques completos: {complete_count} de {total}")

        new_status = status
        new_down_ts = down_ts

        if complete_count > 0 and complete_count < total:
            new_status = "PALLET*EN*CAMINO****"
        elif total > 0 and complete_count == total:
            new_status = "FINALIZADA**********"
            new_down_ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            # Al finalizar la misión se actualizan variables globales para control de alternancia
            mission_in_progress = False
            fase = determine_phase(from_slot)
            if fase is not None:
                last_mission_phase = fase
                last_mission_end_time = time.time()

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
    while getattr(threading.current_thread(), "do_run", True):
        try:
            update_statuses()
        except Exception as e:
            log_error(f"Error en monitor_status_loop: {str(e)}")
        time.sleep(5)

# ------------------ Inicio de los Hilos ------------------

if __name__ == "__main__":
    sender_thread = threading.Thread(target=sender_loop, daemon=True)
    monitor_thread = threading.Thread(target=monitor_status_loop, daemon=True)
    
    sender_thread.start()
    monitor_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo los hilos...")
        sender_thread.do_run = False
        monitor_thread.do_run = False
        sys.exit(0)
