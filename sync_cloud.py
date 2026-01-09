"""
Módulo de sincronización de base de datos en la nube
Soporta múltiples backends: jsonbin.io, Firebase, GitHub, etc.
"""

import sqlite3
import json
import logging
import os
from datetime import datetime
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = "jornada_laboral.db"
BACKUP_DIR = "backups_cloud"
SYNC_CONFIG_FILE = "sync_config.json"
HISTORIAL_SYNC_FILE = "historial_sync.json"  # Nuevo: historial de sincronizaciones

# ============= CONFIGURACIÓN AUTOMÁTICA =============
# URL de Firebase (configurada automáticamente)
FIREBASE_URL = "https://database-checador-default-rtdb.firebaseio.com"
# Backend por defecto
BACKEND_DEFECTO = "firebase"


def cargar_configuracion_sync():
    """Carga la configuración de sincronización desde archivo local
    Si no existe, usa automáticamente Firebase con la URL configurada"""
    if os.path.exists(SYNC_CONFIG_FILE):
        try:
            with open(SYNC_CONFIG_FILE, 'r') as f:
                config = json.load(f)
                # Si está vacía o en modo local, actualizar a Firebase automáticamente
                if config.get("backend") == "local" or not config.get("backend"):
                    config["backend"] = BACKEND_DEFECTO
                    config["url"] = FIREBASE_URL
                    guardar_configuracion_sync(config)
                return config
        except Exception as e:
            logger.warning(f"No se pudo leer configuración de sync: {e}")
    
    # Configuración por defecto: Firebase automático
    return {
        "backend": BACKEND_DEFECTO,
        "url": FIREBASE_URL,
        "api_key": "",
        "bucket_id": ""
    }


def guardar_configuracion_sync(config):
    """Guarda la configuración de sincronización"""
    try:
        with open(SYNC_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        logger.info("Configuración de sync guardada")
        return True
    except Exception as e:
        logger.error(f"Error guardando configuración: {e}")
        return False


def guardar_en_historial(tipo_sync, backend, exito, detalles=""):
    """Guarda un registro en el historial de sincronizaciones"""
    try:
        # Cargar historial existente
        historial = []
        if os.path.exists(HISTORIAL_SYNC_FILE):
            with open(HISTORIAL_SYNC_FILE, 'r') as f:
                historial = json.load(f)
        
        # Agregar nuevo registro
        registro = {
            "timestamp": datetime.now().isoformat(),
            "tipo": tipo_sync,  # "upload" o "download"
            "backend": backend,
            "exito": exito,
            "detalles": detalles
        }
        historial.append(registro)
        
        # Guardar máximo 50 registros
        if len(historial) > 50:
            historial = historial[-50:]
        
        with open(HISTORIAL_SYNC_FILE, 'w') as f:
            json.dump(historial, f, indent=4)
        
        logger.info(f"Registro agregado al historial: {tipo_sync} ({backend})")
        return True
    except Exception as e:
        logger.error(f"Error guardando en historial: {e}")
        return False


def obtener_historial_sync():
    """Obtiene el historial de todas las sincronizaciones"""
    try:
        if os.path.exists(HISTORIAL_SYNC_FILE):
            with open(HISTORIAL_SYNC_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Error leyendo historial: {e}")
        return []


def limpiar_historial():
    """Limpia el historial de sincronizaciones"""
    try:
        if os.path.exists(HISTORIAL_SYNC_FILE):
            os.remove(HISTORIAL_SYNC_FILE)
        logger.info("Historial limpiado")
        return True
    except Exception as e:
        logger.error(f"Error limpiando historial: {e}")
        return False


def inicializar_firebase_automaticamente():
    """Inicializa la configuración de Firebase automáticamente"""
    config = cargar_configuracion_sync()
    
    # Si está configurado correctamente, no hacer nada
    if config.get("backend") == "firebase" and config.get("url") == FIREBASE_URL:
        logger.info("Firebase automático configurado correctamente")
        return True
    
    # Si no, configurar automáticamente
    config["backend"] = BACKEND_DEFECTO
    config["url"] = FIREBASE_URL
    guardar_configuracion_sync(config)
    logger.info(f"Firebase automático configurado: {FIREBASE_URL}")
    return True


def exportar_bd_json():
    """Exporta la base de datos completa a JSON"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        datos = {}
        
        # Exportar tabla empleados
        cursor.execute("SELECT * FROM empleados")
        datos['empleados'] = [dict(row) for row in cursor.fetchall()]
        
        # Exportar tabla tolerancias_empleado
        cursor.execute("SELECT * FROM tolerancias_empleado")
        datos['tolerancias'] = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        logger.info(f"BD exportada: {len(datos['empleados'])} empleados, {len(datos['tolerancias'])} tolerancias")
        return datos
    except Exception as e:
        logger.error(f"Error exportando BD: {e}")
        return None


def importar_bd_json(datos):
    """Importa datos JSON a la base de datos local"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if not datos or 'empleados' not in datos:
            logger.error("Datos inválidos para importar")
            return False
        
        # Crear directorio de backups si no existe
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        # Backup de la BD actual antes de importar
        backup_path = os.path.join(BACKUP_DIR, f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        import shutil
        shutil.copy(DB_PATH, backup_path)
        logger.info(f"Backup creado: {backup_path}")
        
        # Limpiar tablas
        cursor.execute("DELETE FROM tolerancias_empleado")
        cursor.execute("DELETE FROM empleados")
        
        # Importar empleados
        for emp in datos.get('empleados', []):
            cursor.execute('''
                INSERT INTO empleados 
                (id_empleado, nombre, jornada_tipo, lunes_jueves_entrada, lunes_jueves_salida,
                 viernes_entrada, viernes_salida, sabado_entrada, sabado_salida, 
                 es_servicios_generales, es_administrativo, fecha_registro)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                emp.get('id_empleado'),
                emp.get('nombre'),
                emp.get('jornada_tipo', 'Lunes-Viernes'),
                emp.get('lunes_jueves_entrada', '08:00'),
                emp.get('lunes_jueves_salida', '17:00'),
                emp.get('viernes_entrada', '08:00'),
                emp.get('viernes_salida', '16:00'),
                emp.get('sabado_entrada', '08:00'),
                emp.get('sabado_salida', '13:00'),
                emp.get('es_servicios_generales', 0),
                emp.get('es_administrativo', 0),
                emp.get('fecha_registro')
            ))
        
        # Importar tolerancias
        for tol in datos.get('tolerancias', []):
            cursor.execute('''
                INSERT OR REPLACE INTO tolerancias_empleado 
                (id_empleado, tolerancia_entrada, tolerancia_salida_antes, tolerancia_salida_despues,
                 hora_limite_extra_lj, tolerancia_hora_extra, horas_extra_maximo, viernes_permite_extra,
                 sabado_hora_limite, sabado_tolerancia, sabado_entrada_minima,
                 servicios_tolerancia_entrada, servicios_hora_salida_min, tolerancia_salida_minutos_minimos)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tol.get('id_empleado'),
                tol.get('tolerancia_entrada'),
                tol.get('tolerancia_salida_antes'),
                tol.get('tolerancia_salida_despues'),
                tol.get('hora_limite_extra_lj'),
                tol.get('tolerancia_hora_extra'),
                tol.get('horas_extra_maximo'),
                tol.get('viernes_permite_extra', 0),
                tol.get('sabado_hora_limite'),
                tol.get('sabado_tolerancia'),
                tol.get('sabado_entrada_minima'),
                tol.get('servicios_tolerancia_entrada'),
                tol.get('servicios_hora_salida_min'),
                tol.get('tolerancia_salida_minutos_minimos', 30)
            ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"BD importada: {len(datos['empleados'])} empleados")
        return True
    except Exception as e:
        logger.error(f"Error importando BD: {e}")
        return False


def hacer_backup_local():
    """Crea un backup local de la BD"""
    try:
        # Crear directorio de backups si no existe
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        backup_path = os.path.join(BACKUP_DIR, f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
        import shutil
        shutil.copy(DB_PATH, backup_path)
        logger.info(f"Backup local creado: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"Error creando backup: {e}")
        return None


# ============= BACKEND: jsonbin.io =============
def subir_a_jsonbin(datos, api_key, bin_id=None):
    """Sube la BD a jsonbin.io"""
    try:
        headers = {
            "Content-Type": "application/json",
            "X-Master-Key": api_key
        }
        
        if bin_id:
            # Actualizar bin existente
            url = f"https://api.jsonbin.io/v3/b/{bin_id}"
            response = requests.put(url, json=datos, headers=headers, timeout=10)
        else:
            # Crear nuevo bin
            url = "https://api.jsonbin.io/v3/b"
            response = requests.post(url, json=datos, headers=headers, timeout=10)
            if response.status_code == 201:
                result = response.json()
                bin_id = result.get('metadata', {}).get('id')
                logger.info(f"Nuevo bin creado en jsonbin: {bin_id}")
        
        if response.status_code in [200, 201]:
            logger.info(f"BD subida a jsonbin exitosamente")
            return True, bin_id
        else:
            logger.error(f"Error subiendo a jsonbin: {response.status_code} - {response.text}")
            return False, None
    except Exception as e:
        logger.error(f"Error en jsonbin: {e}")
        return False, None


def descargar_de_jsonbin(api_key, bin_id):
    """Descarga la BD desde jsonbin.io"""
    try:
        headers = {
            "X-Master-Key": api_key
        }
        
        url = f"https://api.jsonbin.io/v3/b/{bin_id}/latest"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            datos = result.get('record', {})
            logger.info(f"BD descargada de jsonbin")
            return datos
        else:
            logger.error(f"Error descargando de jsonbin: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error en jsonbin: {e}")
        return None


# ============= BACKEND: GitHub (via raw.githubusercontent.com) =============
def subir_a_github(datos, token, repo, rama="main"):
    """
    Sube la BD a GitHub (requiere token y acceso de escritura)
    Estructura esperada: usuario/repo en GitHub
    """
    try:
        archivo = "bd_sync.json"
        contenido = json.dumps(datos)
        
        # Convertir a base64
        import base64
        contenido_b64 = base64.b64encode(contenido.encode()).decode()
        
        usuario, nombre_repo = repo.split('/')
        url = f"https://api.github.com/repos/{usuario}/{nombre_repo}/contents/{archivo}"
        
        headers = {
            "Authorization": f"token {token}",
            "Content-Type": "application/json"
        }
        
        # Primero, obtener el SHA del archivo actual (si existe)
        response = requests.get(url, headers=headers, timeout=10)
        sha = None
        if response.status_code == 200:
            sha = response.json().get('sha')
        
        # Preparar payload
        payload = {
            "message": f"Sync BD {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": contenido_b64,
            "branch": rama
        }
        
        if sha:
            payload["sha"] = sha
        
        response = requests.put(url, json=payload, headers=headers, timeout=10)
        
        if response.status_code in [200, 201]:
            logger.info("BD subida a GitHub exitosamente")
            return True
        else:
            logger.error(f"Error subiendo a GitHub: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error en GitHub: {e}")
        return False


def descargar_de_github(usuario, repo, rama="main"):
    """Descarga la BD desde GitHub (sin requerir token para lectura)"""
    try:
        url = f"https://raw.githubusercontent.com/{usuario}/{repo}/{rama}/bd_sync.json"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            datos = response.json()
            logger.info("BD descargada de GitHub")
            return datos
        else:
            logger.error(f"Error descargando de GitHub: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error en GitHub: {e}")
        return None


# ============= Funciones de alto nivel =============
def sincronizar_a_nube(config, nombre_usuario=None):
    """Sincroniza la BD local a la nube según la configuración
    
    Args:
        config: Configuración de sincronización
        nombre_usuario: Nombre del usuario que sube (para Firebase)
    """
    datos = exportar_bd_json()
    if not datos:
        return False, "Error exportando BD"
    
    backend = config.get("backend", "local")
    
    if backend == "jsonbin":
        exito, bin_id = subir_a_jsonbin(datos, config.get("api_key"), config.get("bucket_id"))
        if exito:
            config["bucket_id"] = bin_id
            guardar_configuracion_sync(config)
        return exito, "BD sincronizada a jsonbin" if exito else "Error sincronizando a jsonbin"
    
    elif backend == "github":
        exito = subir_a_github(datos, config.get("api_key"), config.get("url"))
        return exito, "BD sincronizada a GitHub" if exito else "Error sincronizando a GitHub"
    
    elif backend == "firebase":
        exito, timestamp = subir_a_firebase(datos, config.get("url"), nombre_usuario=nombre_usuario)
        return exito, f"BD sincronizada a Firebase ({timestamp})" if exito else "Error sincronizando a Firebase"
    
    elif backend == "local":
        backup = hacer_backup_local()
        guardar_en_historial("upload", "local", backup is not None, backup or "Error creando backup")
        return backup is not None, f"Backup creado: {backup}" if backup else "Error creando backup"
    
    return False, f"Backend no soportado: {backend}"


def sincronizar_desde_nube(config, timestamp=None):
    """Descarga la BD desde la nube y la importa localmente
    
    Args:
        config: Configuración con backend y credenciales
        timestamp: Para Firebase, timestamp específico a descargar (si None, usa el más reciente)
    """
    backend = config.get("backend", "local")
    datos = None
    
    if backend == "jsonbin":
        datos = descargar_de_jsonbin(config.get("api_key"), config.get("bucket_id"))
    
    elif backend == "github":
        url = config.get("url")  # Formato: usuario/repo
        datos = descargar_de_github(url.split('/')[0], url.split('/')[1])
    
    elif backend == "firebase":
        if timestamp is None:
            # Retornar lista de backups disponibles
            resultado = descargar_de_firebase(config.get("url"))
            if resultado and resultado.get("tipo") == "lista":
                return True, resultado
            else:
                return False, "Error obteniendo lista de backups"
        else:
            # Descargar backup específico
            resultado = descargar_de_firebase(config.get("url"), timestamp)
            if resultado and resultado.get("tipo") == "datos":
                datos = resultado.get("datos")
            else:
                return False, f"Error descargando backup {timestamp}"
    
    elif backend == "local":
        return False, "No hay nube configurada para descargar"
    
    if datos:
        exito = importar_bd_json(datos)
        return exito, "BD importada desde nube" if exito else "Error importando BD"
    
    return False, f"Error descargando datos de {backend}"


def obtener_resumen_sync():
    """Obtiene un resumen del estado de sincronización"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM empleados")
        total_empleados = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tolerancias_empleado")
        total_tolerancias = cursor.fetchone()[0]
        
        conn.close()
        
        config = cargar_configuracion_sync()
        
        return {
            "total_empleados": total_empleados,
            "total_tolerancias": total_tolerancias,
            "backend": config.get("backend"),
            "sincronizado": True
        }
    except Exception as e:
        logger.error(f"Error obteniendo resumen: {e}")
        return None


# ============= BACKEND: Firebase (Realtime Database) =============
def subir_a_firebase(datos, url_firebase, nombre_usuario=None):
    """
    Sube la BD a Firebase Realtime Database
    Requiere URL del proyecto Firebase: https://[PROJECT_ID].firebaseio.com
    La BD se sube en: /bd_sync/historial/[TIMESTAMP].json
    
    Args:
        datos: Datos de la BD a subir
        url_firebase: URL de Firebase
        nombre_usuario: Nombre del usuario que sube
    """
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # URL para guardar en historial
        url = f"{url_firebase}/bd_sync/historial/{timestamp}.json"
        
        # Agregar metadata
        datos_con_meta = {
            "timestamp": datetime.now().isoformat(),
            "usuario": nombre_usuario or "Desconocido",
            "empleados_count": len(datos.get('empleados', [])),
            "tolerancias_count": len(datos.get('tolerancias', [])),
            "datos": datos
        }
        
        response = requests.put(url, json=datos_con_meta, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"BD subida a Firebase: {timestamp}")
            guardar_en_historial("upload", "firebase", True, timestamp)
            return True, timestamp
        else:
            logger.error(f"Error subiendo a Firebase: {response.status_code}")
            guardar_en_historial("upload", "firebase", False, f"HTTP {response.status_code}")
            return False, None
    except Exception as e:
        logger.error(f"Error en Firebase: {e}")
        guardar_en_historial("upload", "firebase", False, str(e))
        return False, None


def descargar_de_firebase(url_firebase, timestamp=None):
    """
    Descarga la BD desde Firebase
    Si timestamp es None, obtiene el más reciente
    Retorna lista de timestamps disponibles o los datos si timestamp se especifica
    """
    try:
        # Si no se especifica timestamp, obtener lista de disponibles
        if timestamp is None:
            url = f"{url_firebase}/bd_sync/historial.json"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                historial_db = response.json()
                if historial_db:
                    # Retornar lista de timestamps ordenados descendentemente con info de usuario
                    backups_info = []
                    for timestamp in sorted(historial_db.keys(), reverse=True):
                        backup_data = historial_db[timestamp]
                        usuario = backup_data.get('usuario', 'Desconocido') if isinstance(backup_data, dict) else 'N/A'
                        backups_info.append({
                            'timestamp': timestamp,
                            'usuario': usuario
                        })
                    logger.info(f"Backups disponibles en Firebase: {len(backups_info)}")
                    return {"tipo": "lista", "backups": backups_info}
                else:
                    logger.warning("No hay backups en Firebase")
                    return {"tipo": "lista", "backups": []}
            else:
                logger.error(f"Error obteniendo historial de Firebase: {response.status_code}")
                return None
        else:
            # Descargar backup específico
            url = f"{url_firebase}/bd_sync/historial/{timestamp}.json"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                datos_completos = response.json()
                datos = datos_completos.get('datos', {})
                logger.info(f"BD descargada de Firebase: {timestamp}")
                guardar_en_historial("download", "firebase", True, timestamp)
                return {"tipo": "datos", "datos": datos, "timestamp": timestamp}
            else:
                logger.error(f"Error descargando de Firebase: {response.status_code}")
                guardar_en_historial("download", "firebase", False, f"HTTP {response.status_code}")
                return None
    except Exception as e:
        logger.error(f"Error en Firebase: {e}")
        guardar_en_historial("download", "firebase", False, str(e))
        return None


def limpiar_historial_firebase(url_firebase):
    """Limpia todos los backups del historial en Firebase"""
    try:
        url = f"{url_firebase}/bd_sync/historial.json"
        response = requests.delete(url, timeout=10)
        
        if response.status_code == 200:
            logger.info("Historial de Firebase limpiado")
            return True
        else:
            logger.error(f"Error limpiando Firebase: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error en Firebase: {e}")
        return False


# ============= INICIALIZACIÓN AUTOMÁTICA =============
# Al importar este módulo, inicializar Firebase automáticamente
try:
    inicializar_firebase_automaticamente()
except Exception as e:
    logger.warning(f"Error durante inicialización automática de Firebase: {e}")