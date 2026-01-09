
import sqlite3
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

DB_PATH = "jornada_laboral.db"


def agregar_columnas_faltantes():
    """Agrega columnas faltantes a tablas existentes (migración)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Verificar y agregar tolerancia_salida_minutos_minimos si no existe
        cursor.execute("PRAGMA table_info(tolerancias_empleado)")
        columnas = [col[1] for col in cursor.fetchall()]
        
        if 'tolerancia_salida_minutos_minimos' not in columnas:
            cursor.execute('''
                ALTER TABLE tolerancias_empleado 
                ADD COLUMN tolerancia_salida_minutos_minimos INTEGER DEFAULT 30
            ''')
            logger.info("Agregada columna tolerancia_salida_minutos_minimos")
        
        if 'comida_tolerancia_salida' not in columnas:
            cursor.execute('''
                ALTER TABLE tolerancias_empleado 
                ADD COLUMN comida_tolerancia_salida INTEGER DEFAULT 15
            ''')
            logger.info("Agregada columna comida_tolerancia_salida")
        
        # Verificar y agregar es_chofer si no existe en empleados
        cursor.execute("PRAGMA table_info(empleados)")
        columnas_emp = [col[1] for col in cursor.fetchall()]
        
        if 'es_chofer' not in columnas_emp:
            cursor.execute('''
                ALTER TABLE empleados 
                ADD COLUMN es_chofer INTEGER DEFAULT 0
            ''')
            logger.info("Agregada columna es_chofer a empleados")
        
        if 'es_administrativo' not in columnas_emp:
            cursor.execute('''
                ALTER TABLE empleados 
                ADD COLUMN es_administrativo INTEGER DEFAULT 0
            ''')
            logger.info("Agregada columna es_administrativo a empleados")
        
        conn.commit()
    except Exception as e:
        logger.warning(f"Error en migración: {e}")
    finally:
        conn.close()


def inicializar_bd():
    """Crea la base de datos si no existe"""
    if not os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Tabla de empleados y sus jornadas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS empleados (
                id_empleado TEXT PRIMARY KEY,
                nombre TEXT,
                jornada_tipo TEXT DEFAULT 'Lunes-Viernes',
                lunes_jueves_entrada TEXT DEFAULT '08:00',
                lunes_jueves_salida TEXT DEFAULT '17:00',
                viernes_entrada TEXT DEFAULT '08:00',
                viernes_salida TEXT DEFAULT '16:00',
                sabado_entrada TEXT DEFAULT '08:00',
                sabado_salida TEXT DEFAULT '13:00',
                es_servicios_generales INTEGER DEFAULT 0,
                es_administrativo INTEGER DEFAULT 0,
                es_chofer INTEGER DEFAULT 0,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabla de tolerancias personalizadas por empleado
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tolerancias_empleado (
                id_empleado TEXT PRIMARY KEY,
                tolerancia_entrada INTEGER,
                tolerancia_salida_antes INTEGER,
                tolerancia_salida_despues INTEGER,
                hora_limite_extra_lj TEXT,
                tolerancia_hora_extra INTEGER,
                horas_extra_maximo INTEGER,
                viernes_permite_extra INTEGER DEFAULT 0,
                sabado_hora_limite TEXT,
                sabado_tolerancia INTEGER,
                sabado_entrada_minima TEXT,
                servicios_tolerancia_entrada INTEGER,
                servicios_hora_salida_min TEXT,
                tolerancia_salida_minutos_minimos INTEGER DEFAULT 30,
                comida_tolerancia_salida INTEGER DEFAULT 15,
                FOREIGN KEY (id_empleado) REFERENCES empleados(id_empleado) ON DELETE CASCADE
            )
        ''')
        
        # Tabla de permisos (ingreso por PIN = permiso)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS permisos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_empleado TEXT,
                fecha DATE,
                tipo_permiso TEXT,
                hora TEXT,
                tipo_acceso TEXT DEFAULT 'PIN',
                observaciones TEXT,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_empleado) REFERENCES empleados(id_empleado) ON DELETE CASCADE
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Base de datos creada exitosamente")
    else:
        # Base de datos existe, ejecutar migración para columnas faltantes
        agregar_columnas_faltantes()


def obtener_empleado(id_empleado):
    """Obtiene los datos de un empleado"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM empleados WHERE id_empleado = ?", (id_empleado,))
    resultado = cursor.fetchone()
    conn.close()
    return resultado


def guardar_empleado(id_empleado, nombre, jornada_tipo, lunes_jueves_entrada, 
                     lunes_jueves_salida, viernes_entrada, viernes_salida, 
                     sabado_entrada, sabado_salida, es_servicios_generales=False, es_chofer=False, es_administrativo=False):
    """Guarda o actualiza un empleado"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Primero, obtener la fecha_registro actual si existe
        cursor.execute("SELECT fecha_registro FROM empleados WHERE id_empleado = ?", (id_empleado,))
        resultado = cursor.fetchone()
        fecha_registro = resultado[0] if resultado else None
        
        cursor.execute('''
            INSERT OR REPLACE INTO empleados 
            (id_empleado, nombre, jornada_tipo, lunes_jueves_entrada, lunes_jueves_salida,
             viernes_entrada, viernes_salida, sabado_entrada, sabado_salida, 
             fecha_registro, es_servicios_generales, es_chofer, es_administrativo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id_empleado, nombre, jornada_tipo, lunes_jueves_entrada, lunes_jueves_salida,
              viernes_entrada, viernes_salida, sabado_entrada, sabado_salida, 
              fecha_registro, int(es_servicios_generales), int(es_chofer), int(es_administrativo)))
        
        conn.commit()
        logger.info(f"Empleado {id_empleado} guardado exitosamente (es_administrativo={int(es_administrativo)})")
        return True
    except Exception as e:
        logger.error(f"Error guardando empleado: {e}")
        return False
    finally:
        conn.close()


def obtener_todos_empleados():
    """Obtiene todos los empleados"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM empleados ORDER BY nombre")
    resultados = cursor.fetchall()
    conn.close()
    return resultados


def eliminar_empleado(id_empleado):
    """Elimina un empleado"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM empleados WHERE id_empleado = ?", (id_empleado,))
        conn.commit()
        logger.info(f"Empleado {id_empleado} eliminado")
        return True
    except Exception as e:
        logger.error(f"Error eliminando empleado: {e}")
        return False
    finally:
        conn.close()


# Inicializar base de datos al importar
inicializar_bd()


def guardar_empleados_desde_reloj(empleados_dict):
    """Guarda múltiples empleados desde el reloj SOLO si no existen"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        count_guardados = 0
        for id_emp, nombre in empleados_dict.items():
            # Verificar si ya existe
            cursor.execute("SELECT id_empleado FROM empleados WHERE id_empleado = ?", (id_emp,))
            existe = cursor.fetchone()
            
            if not existe:
                # Solo insertar si no existe (no sobrescribir)
                cursor.execute('''
                    INSERT INTO empleados 
                    (id_empleado, nombre, jornada_tipo, lunes_jueves_entrada, lunes_jueves_salida,
                     viernes_entrada, viernes_salida, sabado_entrada, sabado_salida, es_servicios_generales)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (id_emp, nombre, 'Lunes-Viernes', '08:00', '17:00', '08:00', '16:00', '08:00', '13:00', 0))
                count_guardados += 1
        
        conn.commit()
        logger.info(f"Se guardaron {count_guardados} nuevos empleados desde el reloj (Total en reloj: {len(empleados_dict)})")
        return True
    except Exception as e:
        logger.error(f"Error guardando empleados del reloj: {e}")
        return False
    finally:
        conn.close()


def obtener_tolerancias_empleado(id_empleado):
    """Obtiene las tolerancias personalizadas de un empleado. Retorna None si no tiene personalizadas."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tolerancias_empleado WHERE id_empleado = ?", (id_empleado,))
    resultado = cursor.fetchone()
    conn.close()
    return resultado


def guardar_tolerancias_empleado(id_empleado, tolerancia_entrada, tolerancia_salida_antes, 
                                  tolerancia_salida_despues, hora_limite_extra_lj, 
                                  tolerancia_hora_extra, horas_extra_maximo, viernes_permite_extra,
                                  sabado_hora_limite, sabado_tolerancia, sabado_entrada_minima,
                                  servicios_tolerancia_entrada, servicios_hora_salida_min,
                                  tolerancia_salida_minutos_minimos=30, comida_tolerancia_salida=15):
    """Guarda o actualiza las tolerancias personalizadas de un empleado"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO tolerancias_empleado 
        (id_empleado, tolerancia_entrada, tolerancia_salida_antes, tolerancia_salida_despues,
         hora_limite_extra_lj, tolerancia_hora_extra, horas_extra_maximo, viernes_permite_extra,
         sabado_hora_limite, sabado_tolerancia, sabado_entrada_minima,
         servicios_tolerancia_entrada, servicios_hora_salida_min, tolerancia_salida_minutos_minimos,
         comida_tolerancia_salida)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (id_empleado, tolerancia_entrada, tolerancia_salida_antes, tolerancia_salida_despues,
          hora_limite_extra_lj, tolerancia_hora_extra, horas_extra_maximo, viernes_permite_extra,
          sabado_hora_limite, sabado_tolerancia, sabado_entrada_minima,
          servicios_tolerancia_entrada, servicios_hora_salida_min, tolerancia_salida_minutos_minimos,
          comida_tolerancia_salida))
    
    conn.commit()
    conn.close()


def eliminar_tolerancias_empleado(id_empleado):
    """Elimina las tolerancias personalizadas de un empleado (volverá a usar las globales)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tolerancias_empleado WHERE id_empleado = ?", (id_empleado,))
    conn.commit()
    conn.close()


def actualizar_administrativo(id_empleado, es_administrativo):
    """Actualiza solo el campo es_administrativo de un empleado"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            "UPDATE empleados SET es_administrativo = ? WHERE id_empleado = ?",
            (int(es_administrativo), id_empleado)
        )
        conn.commit()
        logger.info(f"Empleado {id_empleado} actualizado: es_administrativo={int(es_administrativo)}")
        return True
    except Exception as e:
        logger.error(f"Error actualizando administrativo: {e}")
        return False
    finally:
        conn.close()


def guardar_permiso(id_empleado, fecha, tipo_permiso, hora, observaciones=""):
    """
    Guarda un permiso en la BD
    tipo_permiso: "salida" o "entrada"
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO permisos (id_empleado, fecha, tipo_permiso, hora, observaciones)
            VALUES (?, ?, ?, ?, ?)
        ''', (id_empleado, fecha, tipo_permiso, hora, observaciones))
        
        conn.commit()
        logger.info(f"Permiso registrado: {id_empleado} - {tipo_permiso} a {hora}")
        return True
    except Exception as e:
        logger.error(f"Error guardando permiso: {e}")
        return False
    finally:
        conn.close()


def obtener_permisos_dia(id_empleado, fecha):
    """Obtiene todos los permisos de un empleado para un día específico"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM permisos 
        WHERE id_empleado = ? AND fecha = ?
        ORDER BY hora
    ''', (id_empleado, fecha))
    
    permisos = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return permisos


def obtener_resumen_permisos(id_empleado, fecha_inicio, fecha_fin):
    """Obtiene resumen de permisos en un rango de fechas"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT tipo_permiso, COUNT(*) as cantidad
        FROM permisos 
        WHERE id_empleado = ? AND fecha BETWEEN ? AND ?
        GROUP BY tipo_permiso
    ''', (id_empleado, fecha_inicio, fecha_fin))
    
    resultado = {row['tipo_permiso']: row['cantidad'] for row in cursor.fetchall()}
    conn.close()
    return resultado


def eliminar_permiso(id_permiso):
    """Elimina un permiso por su ID"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM permisos WHERE id = ?", (id_permiso,))
        conn.commit()
        logger.info(f"Permiso {id_permiso} eliminado")
        return True
    except Exception as e:
        logger.error(f"Error eliminando permiso: {e}")
        return False
    finally:
        conn.close()
