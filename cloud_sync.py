"""
Módulo de sincronización de base de datos en la nube
Permite cargar y descargar la base de datos SQLite a/desde un servidor gratuito
"""

import sqlite3
import json
import requests
import os
import logging
from datetime import datetime
import zipfile
from io import BytesIO

logger = logging.getLogger(__name__)

# Usar Supabase Storage (gratuito) o una API simple
# Para este ejemplo, usamos una solución simple con un servidor público
# Alternativamente, puedes usar Firebase, Supabase o Dropbox API

DB_PATH = "jornada_laboral.db"
BACKUP_DIR = "backups"
CONFIG_FILE = "cloud_config.json"


def crear_directorio_backups():
    """Crea directorio de backups si no existe"""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        logger.info(f"Directorio {BACKUP_DIR} creado")


def guardar_config_nube(api_key=None, servidor_url=None):
    """Guarda la configuración de sincronización en la nube"""
    crear_directorio_backups()
    
    config = {
        'api_key': api_key or '',
        'servidor_url': servidor_url or '',
        'ultima_sincronizacion': None
    }
    
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        logger.info("Configuración de nube guardada")
        return True
    except Exception as e:
        logger.error(f"Error guardando configuración: {e}")
        return False


def cargar_config_nube():
    """Carga la configuración de sincronización desde archivo"""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error cargando configuración: {e}")
    
    return {
        'api_key': '',
        'servidor_url': '',
        'ultima_sincronizacion': None
    }


def crear_backup_local(nombre=None):
    """Crea un backup local de la base de datos
    
    Returns:
        str: Ruta del archivo backup creado
    """
    crear_directorio_backups()
    
    if not os.path.exists(DB_PATH):
        logger.error(f"Base de datos {DB_PATH} no encontrada")
        return None
    
    if nombre is None:
        nombre = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    ruta_backup = os.path.join(BACKUP_DIR, nombre)
    
    try:
        # Copiar archivo
        with open(DB_PATH, 'rb') as origen:
            with open(ruta_backup, 'wb') as destino:
                destino.write(origen.read())
        
        logger.info(f"Backup local creado: {ruta_backup}")
        return ruta_backup
    except Exception as e:
        logger.error(f"Error creando backup: {e}")
        return None


def crear_backup_zip():
    """Crea un archivo ZIP con la base de datos para subir a la nube
    
    Returns:
        bytes: Contenido del archivo ZIP en memoria
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"Base de datos {DB_PATH} no encontrada")
        return None
    
    try:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(DB_PATH, arcname='jornada_laboral.db')
        
        zip_buffer.seek(0)
        logger.info("Backup ZIP creado en memoria")
        return zip_buffer.getvalue()
    except Exception as e:
        logger.error(f"Error creando backup ZIP: {e}")
        return None


def descargar_bd_nube(url_descarga):
    """Descarga la base de datos desde la nube
    
    Args:
        url_descarga: URL pública donde está el archivo
    
    Returns:
        bool: True si fue exitoso
    """
    crear_directorio_backups()
    
    try:
        # Crear backup del archivo local actual antes de descargar
        crear_backup_local("backup_antes_descargar.db")
        
        # Descargar archivo
        response = requests.get(url_descarga, timeout=30)
        response.raise_for_status()
        
        # Si es un ZIP, extraer
        if response.headers.get('content-type') == 'application/zip' or url_descarga.endswith('.zip'):
            with zipfile.ZipFile(BytesIO(response.content)) as zf:
                zf.extractall('.')
        else:
            # Es la DB directamente
            with open(DB_PATH, 'wb') as f:
                f.write(response.content)
        
        logger.info("Base de datos descargada exitosamente de la nube")
        return True
    except Exception as e:
        logger.error(f"Error descargando BD de nube: {e}")
        return False


def subir_bd_nube_github(usuario_github, token_github, archivo_zip_path=None):
    """Sube la BD a un repositorio de GitHub (usando GitHub Gists o API)
    
    Args:
        usuario_github: Usuario de GitHub
        token_github: Token personal de GitHub
        archivo_zip_path: Ruta del archivo ZIP a subir (si es None, crea uno nuevo)
    
    Returns:
        dict: Con 'exito' y 'gist_id' o 'gist_url'
    """
    try:
        if archivo_zip_path is None:
            contenido_zip = crear_backup_zip()
            if contenido_zip is None:
                return {'exito': False, 'error': 'No se pudo crear backup ZIP'}
        else:
            with open(archivo_zip_path, 'rb') as f:
                contenido_zip = f.read()
        
        # Convertir a base64 para enviar
        import base64
        contenido_b64 = base64.b64encode(contenido_zip).decode('utf-8')
        
        # Crear Gist (público) con la BD comprimida
        url_gist = "https://api.github.com/gists"
        headers = {
            'Authorization': f'token {token_github}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "description": f"Backup de Jornada Laboral - {fecha_actual}",
            "public": False,
            "files": {
                "jornada_laboral.db.b64": {
                    "content": contenido_b64
                }
            }
        }
        
        response = requests.post(url_gist, json=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        resultado = response.json()
        gist_id = resultado.get('id')
        gist_url = resultado.get('html_url')
        
        logger.info(f"BD subida a GitHub Gist: {gist_url}")
        
        # Guardar gist_id en config
        config = cargar_config_nube()
        config['ultimo_gist_id'] = gist_id
        config['ultima_sincronizacion'] = fecha_actual
        guardar_config_nube(config.get('api_key'), config.get('servidor_url'))
        
        return {
            'exito': True,
            'gist_id': gist_id,
            'gist_url': gist_url
        }
    except Exception as e:
        logger.error(f"Error subiendo BD a GitHub: {e}")
        return {'exito': False, 'error': str(e)}


def descargar_bd_github_gist(gist_id, token_github):
    """Descarga la BD desde un Gist de GitHub
    
    Args:
        gist_id: ID del Gist
        token_github: Token personal de GitHub
    
    Returns:
        bool: True si fue exitoso
    """
    try:
        crear_directorio_backups()
        crear_backup_local("backup_antes_descargar_github.db")
        
        url_gist = f"https://api.github.com/gists/{gist_id}"
        headers = {
            'Authorization': f'token {token_github}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        response = requests.get(url_gist, headers=headers, timeout=30)
        response.raise_for_status()
        
        gist_data = response.json()
        
        # Encontrar el archivo .db.b64
        for filename, file_info in gist_data.get('files', {}).items():
            if filename.endswith('.db.b64'):
                contenido_b64 = file_info.get('content')
                
                # Decodificar base64
                import base64
                contenido_zip = base64.b64decode(contenido_b64)
                
                # Extraer del ZIP
                with zipfile.ZipFile(BytesIO(contenido_zip)) as zf:
                    zf.extractall('.')
                
                logger.info("BD descargada exitosamente desde GitHub Gist")
                return True
        
        logger.error("No se encontró archivo .db.b64 en el Gist")
        return False
    except Exception as e:
        logger.error(f"Error descargando BD de GitHub Gist: {e}")
        return False


def sincronizar_con_simple_api(servidor_url, api_key, accion='subir'):
    """Sincroniza con un servidor simple (requiere endpoint propio)
    
    Args:
        servidor_url: URL del servidor (ej: https://ejemplo.com/api)
        api_key: Clave API para autenticación
        accion: 'subir' o 'descargar'
    
    Returns:
        dict: Resultado de la sincronización
    """
    try:
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/octet-stream'
        }
        
        if accion == 'subir':
            contenido_zip = crear_backup_zip()
            if not contenido_zip:
                return {'exito': False, 'error': 'No se pudo crear backup'}
            
            response = requests.post(
                f"{servidor_url}/upload",
                data=contenido_zip,
                headers=headers,
                timeout=30
            )
        else:  # descargar
            response = requests.get(
                f"{servidor_url}/download",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                crear_backup_local("backup_antes_descargar_api.db")
                
                with zipfile.ZipFile(BytesIO(response.content)) as zf:
                    zf.extractall('.')
        
        response.raise_for_status()
        logger.info(f"Sincronización {accion} exitosa")
        return {'exito': True, 'mensaje': f'Sincronización {accion} completada'}
    except Exception as e:
        logger.error(f"Error en sincronización {accion}: {e}")
        return {'exito': False, 'error': str(e)}


def listar_backups_locales():
    """Lista todos los backups locales disponibles
    
    Returns:
        list: Lista de nombres de archivos backup
    """
    crear_directorio_backups()
    
    try:
        backups = []
        for archivo in os.listdir(BACKUP_DIR):
            if archivo.endswith('.db'):
                ruta_completa = os.path.join(BACKUP_DIR, archivo)
                tamaño = os.path.getsize(ruta_completa)
                backups.append({
                    'nombre': archivo,
                    'tamaño_bytes': tamaño,
                    'ruta': ruta_completa
                })
        return sorted(backups, key=lambda x: x['nombre'], reverse=True)
    except Exception as e:
        logger.error(f"Error listando backups: {e}")
        return []


def restaurar_backup_local(nombre_backup):
    """Restaura una copia de seguridad local
    
    Args:
        nombre_backup: Nombre del archivo backup
    
    Returns:
        bool: True si fue exitoso
    """
    try:
        ruta_backup = os.path.join(BACKUP_DIR, nombre_backup)
        
        if not os.path.exists(ruta_backup):
            logger.error(f"Backup no encontrado: {ruta_backup}")
            return False
        
        # Crear backup del estado actual
        crear_backup_local("backup_antes_restaurar.db")
        
        # Restaurar
        with open(ruta_backup, 'rb') as origen:
            with open(DB_PATH, 'wb') as destino:
                destino.write(origen.read())
        
        logger.info(f"Base de datos restaurada desde: {nombre_backup}")
        return True
    except Exception as e:
        logger.error(f"Error restaurando backup: {e}")
        return False
