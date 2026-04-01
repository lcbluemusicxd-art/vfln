import customtkinter as ctk
from tkinter import messagebox
import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry
from jornada_laboral import (
    obtener_empleado, guardar_empleado, obtener_todos_empleados,
    eliminar_empleado, guardar_empleados_desde_reloj
)
import requests
from requests.auth import HTTPDigestAuth
import urllib3
from datetime import datetime, timedelta
import threading
import logging
import os
import sys
import webbrowser

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IP_RELOJ = "ip"
USER = "admin"
PASS = "pass*"


def obtener_empleados_del_reloj():
    """Descarga todos los empleados desde el reloj"""
    url = f"https://{IP_RELOJ}/ISAPI/AccessControl/UserInfo/Search?format=json"
    nombres_map = {}
    posicion = 0
    bloque = 100

    while True:
        payload = {
            "UserInfoSearchCond": {
                "searchID": "user_sync_" + datetime.now().strftime("%H%M%S"),
                "searchResultPosition": posicion,
                "maxResults": bloque
            }
        }
        auth = HTTPDigestAuth(USER, PASS)
        res = requests.post(url, json=payload, auth=auth, verify=False, timeout=15)

        if res.status_code == 200:
            data_response = res.json().get('UserInfoSearch', {})
            data = data_response.get('UserInfo', [])

            if not data:
                break

            for u in data:
                id_str = str(u.get('employeeNoString') or u.get('employeeNo') or '')
                nombre = u.get('name', 'Sin Nombre')

                if id_str:
                    nombres_map[id_str] = nombre

            total_matches = data_response.get('totalMatches', 0)
            if posicion + len(data) >= total_matches:
                break

            posicion += len(data)
        else:
            break

    return nombres_map


class GUIJornadaLaboral:
    def __init__(self, root):
        self.root = root
        self.APP_NAME = "VAFERLU Jornada Laboral"
        self.APP_VERSION = "3.0.0"
        # Repo por defecto: lcbluemusicxd-art/vfln (se puede sobreescribir con env var)
        self.UPDATE_REPO = os.environ.get("VAFERLU_UPDATE_REPO", "lcbluemusicxd-art/vfln")  # Formato: owner/repo
        self.root.title(f"{self.APP_NAME} v{self.APP_VERSION}")
        self.root.geometry("1200x750")
        self.root.minsize(1000, 600)

        # Configurar tema
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        # Colores estilo PyCharm
        self.color_bg_main = "#1e1f22"
        self.color_bg_panel = "#2b2d31"
        self.color_bg_input = "#3c3f45"
        self.color_accent = "#4b8fe0"
        self.color_accent_hover = "#5aa0e8"
        self.color_success = "#589c6b"
        self.color_danger = "#d96565"
        self.color_text = "#e8eaed"
        self.color_text_dim = "#a0a1a3"
        self.color_border = "#454749"

        # Sistema de escalado dinámico de fuentes
        self.base_width = 1200
        self.base_height = 750

        # Variables
        self.id_empleado_var = tk.StringVar()
        self.nombre_var = tk.StringVar()
        self.jornada_var = tk.StringVar(value="Lunes-Viernes")
        self.empleados_reloj = {}
        
        # Variables para almacenar datos de asistencia cargados
        self.datos_entrada_salida_actual = None
        self.datos_asistencia_actual = None
        self.fecha_inicio_actual = None
        self.fecha_fin_actual = None

        # Inicializar sabado_frame ANTES de crear la interfaz
        self.sabado_frame = None

        # Inicialización UI
        self.crear_interfaz()
        self._set_app_icon()
        # Splash al iniciar
        self._show_splash_screen()
        # Sincronización inicial
        self.sincronizar_empleados_reloj()

        # Vincular eventos de redimensionamiento
        self.root.bind("<Configure>", self._on_window_resize)

    def _get_font_size(self, base_size):
        """Calcula el tamaño dinámico de fuente basado en el tamaño de la ventana"""
        current_width = self.root.winfo_width()
        if current_width < 1:  # Si la ventana aún no se ha renderizado
            current_width = self.base_width

        scale_factor = current_width / self.base_width
        return max(8, int(base_size * scale_factor))

    def _on_window_resize(self, event=None):
        """Actualiza tamaños de fuente cuando se redimensiona la ventana"""
        pass  # Puedes agregar lógica adicional aquí si lo necesitas

    def crear_interfaz(self):
        """Crea la interfaz gráfica estilo PyCharm"""

        # Frame principal
        main_container = ctk.CTkFrame(self.root, fg_color=self.color_bg_main)
        main_container.pack(fill=tk.BOTH, expand=True)

        # ===== HEADER =====
        header = ctk.CTkFrame(main_container, fg_color=self.color_bg_panel, height=70)
        header.pack(fill=tk.X, padx=0, pady=0)
        header.pack_propagate(False)

        header_content = ctk.CTkFrame(header, fg_color="transparent")
        header_content.pack(fill=tk.BOTH, expand=True, padx=25, pady=12)

        title = ctk.CTkLabel(header_content, text=f"{self.APP_NAME}",
                            font=("JetBrains Mono", 28, "bold"), text_color=self.color_text)
        title.pack(side=tk.LEFT, anchor=tk.W)

        button_frame = ctk.CTkFrame(header_content, fg_color="transparent")
        button_frame.pack(side=tk.RIGHT, anchor=tk.E)

        # --- SELECTOR DE FECHAS ---
        fecha_frame = ctk.CTkFrame(button_frame, fg_color="transparent")
        fecha_frame.pack(side=tk.LEFT, padx=(0, 20))

        ctk.CTkLabel(fecha_frame, text="Desde:",
                    font=("Segoe UI", 16), text_color=self.color_text).pack(side=tk.LEFT, padx=(0, 8))
        
        # DateEntry moderno con calendario
        self.fecha_inicio_entry = DateEntry(fecha_frame, 
                                           date_pattern='dd-mm-yyyy',
                                           locale='es_ES',
                                           background=self.color_accent,
                                           foreground='white',
                                           borderwidth=0,
                                           font=("Segoe UI", 16),
                                           width=12,
                                           headersbackground=self.color_accent,
                                           headersforeground='white',
                                           selectbackground=self.color_accent,
                                           selectforeground='white',
                                           normalbackground=self.color_bg_panel,
                                           normalforeground=self.color_text,
                                           weekendbackground='#3c3f45',
                                           weekendforeground=self.color_text,
                                           othermonthforeground=self.color_text_dim,
                                           othermonthbackground=self.color_bg_input,
                                           othermonthweforeground=self.color_text_dim,
                                           othermonthwebackground=self.color_bg_input)
        self.fecha_inicio_entry.set_date(datetime.now() - timedelta(days=30))
        self.fecha_inicio_entry.pack(side=tk.LEFT, padx=(0, 15))
        self.fecha_inicio_entry.bind("<<DateEntrySelected>>", lambda e: self.sincronizar_empleados_reloj())

        ctk.CTkLabel(fecha_frame, text="Hasta:",
                    font=("Segoe UI", 16), text_color=self.color_text).pack(side=tk.LEFT, padx=(0, 8))
        
        # DateEntry moderno con calendario
        self.fecha_fin_entry = DateEntry(fecha_frame,
                                        date_pattern='dd-mm-yyyy',
                                        locale='es_ES',
                                        background=self.color_accent,
                                        foreground='white',
                                        borderwidth=0,
                                        font=("Segoe UI", 16),
                                        width=12,
                                        headersbackground=self.color_accent,
                                        headersforeground='white',
                                        selectbackground=self.color_accent,
                                        selectforeground='white',
                                        normalbackground=self.color_bg_panel,
                                        normalforeground=self.color_text,
                                        weekendbackground='#3c3f45',
                                        weekendforeground=self.color_text,
                                        othermonthforeground=self.color_text_dim,
                                        othermonthbackground=self.color_bg_input,
                                        othermonthweforeground=self.color_text_dim,
                                        othermonthwebackground=self.color_bg_input)
        self.fecha_fin_entry.set_date(datetime.now())
        self.fecha_fin_entry.pack(side=tk.LEFT, padx=(0, 15))
        self.fecha_fin_entry.bind("<<DateEntrySelected>>", lambda e: self.sincronizar_empleados_reloj())

        btn_update = ctk.CTkButton(button_frame, text="Buscar Actualización",
                       command=self.check_for_updates,
                       font=("Segoe UI", 12, "bold"),
                       fg_color="#16a085",
                       hover_color="#1abc9c",
                       text_color="white",
                       width=160, height=36, corner_radius=6)
        btn_update.pack(side=tk.LEFT, padx=8)

        btn_sync = ctk.CTkButton(button_frame, text="Sincronizar Reloj",
                                command=self.sincronizar_empleados_reloj,
                                font=("Segoe UI", 12, "bold"),
                                fg_color=self.color_accent,
                                hover_color=self.color_accent_hover,
                                text_color="white",
                                width=150, height=36, corner_radius=6)
        btn_sync.pack(side=tk.LEFT, padx=8)

        self.status_label = ctk.CTkLabel(button_frame, text="Listo",
                                        font=("Segoe UI", 13),
                                        text_color=self.color_success)
        self.status_label.pack(side=tk.LEFT, padx=20)

        # Línea separadora
        sep_h = ctk.CTkFrame(main_container, fg_color=self.color_border, height=1)
        sep_h.pack(fill=tk.X, padx=0, pady=0)

        # ===== CONTENIDO PRINCIPAL =====
        content = ctk.CTkFrame(main_container, fg_color=self.color_bg_main)
        content.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # --- PANEL IZQUIERDO: FORMULARIO ---
        left_panel = ctk.CTkFrame(content, fg_color=self.color_bg_panel, corner_radius=8)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        # Título izquierdo
        left_title = ctk.CTkLabel(left_panel, text="CONFIGURAR EMPLEADO",
                                 font=("Segoe UI", 16, "bold"),
                                 text_color=self.color_accent)
        left_title.pack(anchor=tk.W, padx=20, pady=(15, 10))

        # Separador título
        sep_left = ctk.CTkFrame(left_panel, fg_color=self.color_border, height=1)
        sep_left.pack(fill=tk.X, padx=20, pady=(0, 15))

        # Scroll frame
        scroll_frame = ctk.CTkScrollableFrame(left_panel, fg_color="transparent")
        scroll_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        # === DATOS PERSONALES ===
        data_label = ctk.CTkLabel(scroll_frame, text="Datos Personales",
                                 font=("Segoe UI", 16, "bold"),
                                 text_color=self.color_text_dim)
        data_label.pack(anchor=tk.W, pady=(0, 6))

        # ID
        ctk.CTkLabel(scroll_frame, text="ID Empleado",
                    font=("Segoe UI", 14), text_color=self.color_text).pack(anchor=tk.W, pady=(0, 2))
        self.id_entry = ctk.CTkComboBox(scroll_frame, variable=self.id_empleado_var,
                                       font=("Segoe UI", 14),
                                       command=self.cargar_empleado_por_id,
                                       fg_color=self.color_bg_input,
                                       border_color=self.color_border, height=34)
        self.id_entry.pack(fill=tk.X, pady=(0, 8))
        self.id_entry.bind("<KeyRelease>", self.filtrar_ids)

        # Nombre
        ctk.CTkLabel(scroll_frame, text="Nombre Completo",
                    font=("Segoe UI", 14), text_color=self.color_text).pack(anchor=tk.W, pady=(0, 2))
        self.nombre_entry = ctk.CTkEntry(scroll_frame, textvariable=self.nombre_var,
                                        font=("Segoe UI", 14),
                                        fg_color=self.color_bg_input,
                                        border_color=self.color_border, height=34)
        self.nombre_entry.pack(fill=tk.X, pady=(0, 8))

        # Jornada
        ctk.CTkLabel(scroll_frame, text="Tipo de Jornada",
                    font=("Segoe UI", 14), text_color=self.color_text).pack(anchor=tk.W, pady=(0, 2))
        jornada_combo = ctk.CTkComboBox(scroll_frame, variable=self.jornada_var,
                                       values=["Lunes-Viernes", "Lunes-Sabado"],
                                       command=self.actualizar_campos,
                                       font=("Segoe UI", 14),
                                       state="readonly",
                                       fg_color=self.color_bg_input,
                                       border_color=self.color_border, height=34)
        jornada_combo.pack(fill=tk.X, pady=(0, 10))

        # Separador
        sep_data = ctk.CTkFrame(scroll_frame, fg_color=self.color_border, height=1)
        sep_data.pack(fill=tk.X, pady=8)

        # === HORARIOS ===
        horarios_label = ctk.CTkLabel(scroll_frame, text="Horarios de Trabajo",
                                     font=("Segoe UI", 16, "bold"),
                                     text_color=self.color_text_dim)
        horarios_label.pack(anchor=tk.W, pady=(8, 6))

        self._create_time_row(scroll_frame, "Lunes - Jueves", "lj", "08:00", "17:00")
        self._create_time_row(scroll_frame, "Viernes", "v", "08:00", "16:00")
        self._create_time_row(scroll_frame, "Sábado", "s", "08:00", "13:00", disabled=True)

        # Botones
        buttons_frame = ctk.CTkFrame(left_panel, fg_color="transparent")
        buttons_frame.pack(fill=tk.X, padx=20, pady=(8, 20))

        btn_save = ctk.CTkButton(buttons_frame, text="Guardar",
                                command=self.guardar,
                                font=("Segoe UI", 14, "bold"),
                                fg_color=self.color_success,
                                hover_color="#6ba878",
                                text_color="white",
                                height=40, corner_radius=6)
        btn_save.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        btn_clear = ctk.CTkButton(buttons_frame, text="Limpiar",
                                 command=self.limpiar,
                                 font=("Segoe UI", 14, "bold"),
                                 fg_color=self.color_bg_input,
                                 hover_color=self.color_border,
                                 text_color=self.color_text,
                                 height=40, corner_radius=6)
        btn_clear.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        btn_delete = ctk.CTkButton(buttons_frame, text="Eliminar",
                                  command=self.eliminar,
                                  font=("Segoe UI", 14, "bold"),
                                  fg_color=self.color_danger,
                                  hover_color="#e07878",
                                  text_color="white",
                                  height=40, corner_radius=6)
        btn_delete.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        # --- BOTONES DE REPORTES ---
        reports_frame = ctk.CTkFrame(left_panel, fg_color="transparent")
        reports_frame.pack(fill=tk.X, padx=20, pady=(8, 20))

        btn_reporte_detallado = ctk.CTkButton(
            reports_frame, text="Reporte Detallado\nsin Descuentos",
            command=self.generar_reporte_detallado,
            font=("Segoe UI", 12, "bold"),
            fg_color="#5B9BD5",
            hover_color="#7CB5E5",
            text_color="white",
            height=40, corner_radius=6)
        btn_reporte_detallado.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        btn_reporte_asistencia = ctk.CTkButton(
            reports_frame, text="Reporte de\nAsistencia",
            command=self.generar_reporte_asistencia,
            font=("Segoe UI", 12, "bold"),
            fg_color="#5B9BD5",
            hover_color="#7CB5E5",
            text_color="white",
            height=40, corner_radius=6)
        btn_reporte_asistencia.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        btn_reporte_nomina = ctk.CTkButton(
            reports_frame, text="Reporte de\nNómina",
            command=self.generar_reporte_nomina,
            font=("Segoe UI", 12, "bold"),
            fg_color="#5B9BD5",
            hover_color="#7CB5E5",
            text_color="white",
            height=40, corner_radius=6)
        btn_reporte_nomina.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        # --- BOTONES DE SINCRONIZACIÓN EN LA NUBE ---
        cloud_frame = ctk.CTkFrame(left_panel, fg_color="transparent")
        cloud_frame.pack(fill=tk.X, padx=20, pady=(8, 20))

        btn_descargar_bd = ctk.CTkButton(
            cloud_frame, text="⬇ Descargar BD\nde la Nube",
            command=self.descargar_bd_nube,
            font=("Segoe UI", 11, "bold"),
            fg_color="#27ae60",
            hover_color="#229954",
            text_color="white",
            height=40, corner_radius=6)
        btn_descargar_bd.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        btn_subir_bd = ctk.CTkButton(
            cloud_frame, text="⬆ Subir BD\na la Nube",
            command=self.subir_bd_nube,
            font=("Segoe UI", 11, "bold"),
            fg_color="#2980b9",
            hover_color="#1f618d",
            text_color="white",
            height=40, corner_radius=6)
        btn_subir_bd.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        btn_backups = ctk.CTkButton(
            cloud_frame, text="📦 Gestionar\nBackups",
            command=self.gestionar_backups,
            font=("Segoe UI", 11, "bold"),
            fg_color="#8e44ad",
            hover_color="#6c3483",
            text_color="white",
            height=40, corner_radius=6)
        btn_backups.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)

        # --- PANEL DERECHO: TABLA ---
        right_panel = ctk.CTkFrame(content, fg_color=self.color_bg_panel, corner_radius=8)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(10, 0))

        # Frame para título y botón de configuración
        title_frame = ctk.CTkFrame(right_panel, fg_color="transparent")
        title_frame.pack(anchor=tk.W, fill=tk.X, padx=20, pady=(15, 10))
        
        # Título derecho
        right_title = ctk.CTkLabel(title_frame, text="EMPLEADOS REGISTRADOS",
                                  font=("Segoe UI", 16, "bold"),
                                  text_color=self.color_accent)
        right_title.pack(side=tk.LEFT)
        
        # Botón de configuración (tuerca)
        config_btn = ctk.CTkButton(title_frame, text="⚙", width=40, height=32,
                                   font=("Segoe UI", 20),
                                   fg_color=self.color_accent,
                                   hover_color=self.color_accent_hover,
                                   corner_radius=6,
                                   command=self.abrir_configuracion_tolerancias)
        config_btn.pack(side=tk.RIGHT, padx=(10, 0))
        
        # Botón de configuración de comida/desayuno
        comida_btn = ctk.CTkButton(title_frame, text="🍽", width=40, height=32,
                                   font=("Segoe UI", 20),
                                   fg_color="#e67e22",
                                   hover_color="#d35400",
                                   corner_radius=6,
                                   command=self.abrir_configuracion_comida)
        comida_btn.pack(side=tk.RIGHT, padx=(5, 0))

        # Separador título
        sep_right = ctk.CTkFrame(right_panel, fg_color=self.color_border, height=1)
        sep_right.pack(fill=tk.X, padx=20, pady=(0, 15))

        # Tabla
        table_frame = ctk.CTkFrame(right_panel, fg_color=self.color_bg_main, corner_radius=6)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 20))

        scrollbar = ttk.Scrollbar(table_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree = ttk.Treeview(table_frame, columns=("ID", "Nombre", "Jornada", "L-J", "V", "S"),
                                show="headings", yscrollcommand=scrollbar.set)
        scrollbar.configure(command=self.tree.yview)

        # Estilo tabla
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("Treeview",
                       background=self.color_bg_main,
                       foreground=self.color_text,
                       fieldbackground=self.color_bg_main,
                       font=("Consolas", 14),
                       rowheight=32)
        style.configure("Treeview.Heading",
                       background=self.color_bg_input,
                       foreground=self.color_text,
                       font=("Segoe UI", 13, "bold"))
        style.map('Treeview',
                 background=[('selected', self.color_accent)],
                 foreground=[('selected', 'white')])

        # Columnas
        self.tree.column("ID", width=50, anchor=tk.CENTER)
        self.tree.column("Nombre", width=140, anchor=tk.W)
        self.tree.column("Jornada", width=120, anchor=tk.CENTER)
        self.tree.column("L-J", width=110, anchor=tk.CENTER)
        self.tree.column("V", width=90, anchor=tk.CENTER)
        self.tree.column("S", width=90, anchor=tk.CENTER)

        self.tree.heading("ID", text="ID")
        self.tree.heading("Nombre", text="Nombre")
        self.tree.heading("Jornada", text="Jornada")
        self.tree.heading("L-J", text="Lun-Jue")
        self.tree.heading("V", text="Viernes")
        self.tree.heading("S", text="Sábado")

        self.tree.tag_configure('oddrow', background=self.color_bg_main)
        self.tree.tag_configure('evenrow', background=self.color_bg_input)

        self.tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.tree.bind("<Double-1>", self.seleccionar_empleado)
        self.tree.bind("<Button-1>", self.deseleccionar_si_vacio)
        table_frame.bind("<Button-1>", self.deseleccionar_si_vacio)

        # ===== FOOTER =====
        footer = ctk.CTkFrame(main_container, fg_color=self.color_bg_panel, height=45)
        footer.pack(fill=tk.X, padx=0, pady=0)
        footer.pack_propagate(False)

        footer_label = ctk.CTkLabel(footer, text="Doble clic para editar • Sincronización automática • Los nuevos empleados se agregan a la BD",
                                   font=("Segoe UI", 10),
                                   text_color=self.color_text_dim)
        footer_label.pack(side=tk.LEFT, padx=25, pady=12)

    def resource_path(self, relative_path: str) -> str:
        """Obtiene ruta a recursos, compatible con PyInstaller."""
        base_path = getattr(sys, '_MEIPASS', os.path.abspath(os.path.dirname(__file__)))
        return os.path.join(base_path, relative_path)

    def _apply_icon_to(self, window):
        """Aplica el ícono de marca al window indicado."""
        try:
            # Preferir PNG de alta resolución (transparente V), luego ICO
            png_icon = self.resource_path(os.path.join("assets", "vaferlu.png"))
            if os.path.exists(png_icon):
                try:
                    photo = tk.PhotoImage(file=png_icon)
                    window.iconphoto(True, photo)
                    return
                except Exception:
                    pass
            icon_path = self.resource_path(os.path.join("assets", "vaferlu.ico"))
            if os.path.exists(icon_path):
                try:
                    window.iconbitmap(icon_path)
                    return
                except Exception:
                    pass
        except Exception:
            pass

    def _set_app_icon(self):
        """Configura el ícono en la ventana principal."""
        self._apply_icon_to(self.root)

    def _show_splash_screen(self):
        """Muestra un splash screen breve al iniciar (moderno y estilizado)."""
        try:
            splash = ctk.CTkToplevel(self.root)
            splash.overrideredirect(True)
            splash.configure(fg_color=self.color_bg_panel)
            # Ícono del splash
            self._apply_icon_to(splash)
            w, h = 560, 340
            # Centrar
            x = (self.root.winfo_screenwidth() // 2) - (w // 2)
            y = (self.root.winfo_screenheight() // 2) - (h // 2)
            splash.geometry(f"{w}x{h}+{x}+{y}")

            # Card contenedor con borde/acento
            card = ctk.CTkFrame(splash, fg_color=self.color_bg_main, corner_radius=18, border_width=1,
                                border_color=self.color_border)
            card.place(relx=0.5, rely=0.5, anchor=tk.CENTER, relwidth=0.9, relheight=0.82)

            title = ctk.CTkLabel(card, text=self.APP_NAME, font=("JetBrains Mono", 22, "bold"))
            title.pack(pady=(30, 6))
            subtitle = ctk.CTkLabel(card, text=f"Gestión de Asistencia y Nómina • v{self.APP_VERSION}",
                                    font=("Segoe UI", 13), text_color=self.color_text_dim)
            subtitle.pack(pady=(0, 8))

            divider = ctk.CTkFrame(card, height=1, fg_color=self.color_border)
            divider.pack(fill=tk.X, padx=30, pady=(6, 12))

            detail = ctk.CTkLabel(card, text="VAFERLU • Sincronización de reloj y reportes de nómina",
                                   font=("Segoe UI", 12), text_color=self.color_text_dim, justify=tk.CENTER)
            detail.pack(pady=(0, 12))

            pb = ctk.CTkProgressBar(card, width=360)
            pb.configure(mode="indeterminate", progress_color=self.color_accent, fg_color=self.color_bg_input)
            pb.pack(pady=8)
            pb.start()

            self.root.after(1600, lambda: (pb.stop(), splash.destroy()))
        except Exception:
            # Si falla, continuar sin splash
            pass

    def check_for_updates(self):
        """Verifica si hay una versión nueva en GitHub Releases."""
        if not self.UPDATE_REPO:
            messagebox.showinfo("Actualizaciones", "Repositorio no configurado. Define VAFERLU_UPDATE_REPO = 'owner/repo'.")
            return

        current = self.APP_VERSION
        try:
            url = f"https://api.github.com/repos/{self.UPDATE_REPO}/releases/latest"
            res = requests.get(url, timeout=8)
            if res.status_code == 404:
                messagebox.showinfo("Actualizaciones", "No se encontró actualización")
                return
            if res.status_code != 200:
                messagebox.showinfo("Actualizaciones", "No se encontró actualización")
                return

            data = res.json()
            latest_tag = data.get("tag_name") or data.get("name") or ""
            html_url = data.get("html_url")
            if latest_tag:
                # Normalizar: quitar 'v' inicial
                latest = latest_tag.lstrip('vV')
                if self._is_newer(latest, current):
                    self._show_update_dialog(latest, current, html_url)
                else:
                    messagebox.showinfo("Actualizaciones", "No se encontró actualización")
            else:
                messagebox.showinfo("Actualizaciones", "No se encontró actualización")
        except Exception as e:
            messagebox.showinfo("Actualizaciones", "No se encontró actualización")

    def _is_newer(self, latest: str, current: str) -> bool:
        def parse(v: str):
            parts = [int(p) for p in v.split('.') if p.isdigit() or (p and p[0].isdigit())]
            while len(parts) < 3:
                parts.append(0)
            return tuple(parts[:3])
        return parse(latest) > parse(current)

    def _show_update_dialog(self, latest: str, current: str, html_url: str):
        """Muestra un diálogo estilizado para abrir el release."""
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("Actualización disponible")
        dialog.geometry("420x210")
        dialog.resizable(False, False)
        dialog.configure(fg_color=self.color_bg_main)
        dialog.transient(self.root)
        dialog.grab_set()
        self._apply_icon_to(dialog)

        title = ctk.CTkLabel(dialog, text="Nueva versión disponible",
                             font=("JetBrains Mono", 18, "bold"), text_color=self.color_accent)
        title.pack(pady=(16, 6))

        msg = f"Actual: v{current}\nDisponible: v{latest}\n\n¿Quieres abrir la página de descarga?"
        body = ctk.CTkLabel(dialog, text=msg, justify=tk.LEFT,
                           font=("Segoe UI", 13), text_color=self.color_text)
        body.pack(padx=20, pady=(0, 14))

        btn_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        btn_frame.pack(pady=(0, 12))

        def abrir():
            url = html_url or f"https://github.com/{self.UPDATE_REPO}/releases"
            webbrowser.open(url)
            dialog.destroy()

        btn_abrir = ctk.CTkButton(btn_frame, text="Abrir Release",
                                  command=abrir,
                                  fg_color=self.color_accent,
                                  hover_color=self.color_accent_hover,
                                  width=140)
        btn_abrir.pack(side=tk.LEFT, padx=6)

        btn_luego = ctk.CTkButton(btn_frame, text="Más tarde",
                                  command=dialog.destroy,
                                  fg_color=self.color_bg_input,
                                  hover_color=self.color_border,
                                  text_color=self.color_text,
                                  width=120)
        btn_luego.pack(side=tk.LEFT, padx=6)

    def _create_time_row(self, parent, label, var_prefix, entrada_default, salida_default, disabled=False):
        """Crea una fila de horarios"""
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill=tk.X, pady=5)

        day_label = ctk.CTkLabel(row, text=label,
                                font=("Segoe UI", 14),
                                text_color=self.color_text,
                                width=120, anchor=tk.W)
        day_label.pack(side=tk.LEFT, padx=(0, 12))

        entrada = ctk.CTkEntry(row, width=75, placeholder_text="HH:MM",
                              font=("Consolas", 14),
                              fg_color=self.color_bg_input,
                              border_color=self.color_border, height=34)
        entrada.pack(side=tk.LEFT, padx=3)
        entrada.insert(0, entrada_default)
        if disabled:
            entrada.configure(state="disabled")
        setattr(self, f"{var_prefix}_entrada", entrada)

        sep_label = ctk.CTkLabel(row, text="→",
                                font=("Segoe UI", 14),
                                text_color=self.color_border)
        sep_label.pack(side=tk.LEFT, padx=8)

        salida = ctk.CTkEntry(row, width=75, placeholder_text="HH:MM",
                             font=("Consolas", 14),
                             fg_color=self.color_bg_input,
                             border_color=self.color_border, height=34)
        salida.pack(side=tk.LEFT, padx=3)
        salida.insert(0, salida_default)
        if disabled:
            salida.configure(state="disabled")
        setattr(self, f"{var_prefix}_salida", salida)

        # Guardar referencia al frame del sábado para poder ocultarlo/mostrarlo
        if var_prefix == "s":
            self.sabado_frame = row

    def sincronizar_empleados_reloj(self):
        """Sincroniza empleados del reloj"""
        self.status_label.configure(text="Sincronizando...", text_color="#f39c12")
        self.root.update()

        def _sync():
            self.empleados_reloj = obtener_empleados_del_reloj()
            logger.info(f"Empleados del reloj obtenidos: {list(self.empleados_reloj.items())[:3]}")

            if self.empleados_reloj:
                guardar_empleados_desde_reloj(self.empleados_reloj)
                logger.info(f"Se sincronizaron {len(self.empleados_reloj)} empleados")

                # Recargar la GUI
                self.actualizar_lista_ids()
                self.cargar_empleados()

                self.status_label.configure(
                    text=f"✓ {len(self.empleados_reloj)} empleados",
                    text_color=self.color_success
                )
            else:
                logger.warning("No se obtuvieron empleados del reloj")
                self.status_label.configure(text="✓ Sin cambios", text_color=self.color_success)

        thread = threading.Thread(target=_sync, daemon=True)
        thread.start()

    def actualizar_lista_ids(self):
        ids_ordenados = sorted(self.empleados_reloj.keys(), key=lambda x: int(x) if x.isdigit() else 0)
        self.id_entry.configure(values=ids_ordenados)

    def filtrar_ids(self, event=None):
        valor = self.id_empleado_var.get()
        if valor:
            ids_filtrados = [id for id in self.empleados_reloj.keys() if id.startswith(valor)]
            self.id_entry.configure(values=sorted(ids_filtrados, key=lambda x: int(x) if x.isdigit() else 0))
        else:
            self.actualizar_lista_ids()

    def cargar_empleado_por_id(self, event=None):
        id_emp = self.id_empleado_var.get()
        if not id_emp:
            return

        # Buscar el empleado por ID en la BD
        todos_empleados = obtener_todos_empleados()
        emp_local = None
        
        for emp_data in todos_empleados:
            if emp_data[0] == id_emp:  # emp_data[0] es el ID
                emp_local = emp_data
                break
        
        if emp_local:
            self._llenar_campos(emp_local)
        elif id_emp in self.empleados_reloj:
            self.nombre_var.set(self.empleados_reloj[id_emp])
            self.jornada_var.set("Lunes-Viernes")
            self._reset_horarios()

        self.actualizar_campos()

    def _llenar_campos(self, emp):
        self.id_empleado_var.set(emp[0])
        self.nombre_var.set(emp[1])
        self.jornada_var.set(emp[2])
        self.lj_entrada.delete(0, tk.END)
        self.lj_entrada.insert(0, emp[3])
        self.lj_salida.delete(0, tk.END)
        self.lj_salida.insert(0, emp[4])
        self.v_entrada.delete(0, tk.END)
        self.v_entrada.insert(0, emp[5])
        self.v_salida.delete(0, tk.END)
        self.v_salida.insert(0, emp[6])
        self.s_entrada.delete(0, tk.END)
        self.s_entrada.insert(0, emp[7])
        self.s_salida.delete(0, tk.END)
        self.s_salida.insert(0, emp[8])

    def _reset_horarios(self):
        self.lj_entrada.delete(0, tk.END)
        self.lj_entrada.insert(0, "08:00")
        self.lj_salida.delete(0, tk.END)
        self.lj_salida.insert(0, "17:00")
        self.v_entrada.delete(0, tk.END)
        self.v_entrada.insert(0, "08:00")
        self.v_salida.delete(0, tk.END)
        self.v_salida.insert(0, "16:00")

    def actualizar_campos(self, event=None):
        """Muestra/oculta el sábado según el tipo de jornada"""
        if self.jornada_var.get() == "Lunes-Sabado":
            # Mostrar sábado
            self.s_entrada.configure(state="normal")
            self.s_salida.configure(state="normal")
            if self.sabado_frame:
                self.sabado_frame.pack(fill=tk.X, pady=5)
        else:
            # Ocultar sábado
            self.s_entrada.configure(state="disabled")
            self.s_salida.configure(state="disabled")
            if self.sabado_frame:
                self.sabado_frame.pack_forget()

    def abrir_configuracion_tolerancias(self):
        """Abre diálogo de configuración de tolerancias (globales o individuales)"""
        from jornada_laboral import obtener_tolerancias_empleado, guardar_tolerancias_empleado, eliminar_tolerancias_empleado, obtener_empleado
        import main
        
        # Verificar si hay un empleado seleccionado
        selected = self.tree.selection()
        id_empleado = None
        nombre_empleado = None
        es_servicios_generales = False
        es_administrativo = False
        es_chofer = False
        emp_data = None
        
        if selected:
            item = self.tree.item(selected[0])
            #id_empleado = item['values'][0]  # PROBLEMA: Treeview retorna solo números sin ceros
            nombre_empleado = item['values'][1]
            
            # Buscar el empleado por nombre en la BD para obtener los datos correctos
            todos_empleados = obtener_todos_empleados()
            for emp in todos_empleados:
                if emp[1] == nombre_empleado:  # emp[1] es el nombre
                    emp_data = emp
                    id_empleado = emp[0]  # Obtener el ID con formato correcto (8 dígitos)
                    nombre_empleado = emp[1]  # Asegurar que el nombre sea el correcto de BD
                    break
            
            if emp_data:
                # Estructura: 0-id, 1-nombre, 2-jornada, 3-8 (horas), 9-fecha_registro, 10-es_servicios_generales, 11-es_chofer, 12-es_administrativo
                if len(emp_data) > 10:
                    es_servicios_generales = bool(emp_data[10])  # índice 10 es es_servicios_generales
                if len(emp_data) > 11:
                    es_chofer = bool(emp_data[11])  # índice 11 es es_chofer
                if len(emp_data) > 12:
                    es_administrativo = bool(emp_data[12])  # índice 12 es es_administrativo

        
        # Crear ventana de diálogo - usar self.root en lugar de self
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("Configuración de Tolerancias")
        dialog.geometry("550x750")
        dialog.configure(fg_color=self.color_bg_main)
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Título
        if id_empleado:
            titulo_text = f"Tolerancias de {nombre_empleado}"
        else:
            titulo_text = "Tolerancias Globales"
        
        titulo = ctk.CTkLabel(dialog, text=titulo_text,
                             font=("Segoe UI", 18, "bold"),
                             text_color=self.color_accent)
        titulo.pack(pady=(20, 10))
        
        # Frame scrollable
        scroll_frame = ctk.CTkScrollableFrame(dialog, fg_color=self.color_bg_panel)
        scroll_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # Obtener tolerancias actuales (personalizadas o globales)
        tol_personal = obtener_tolerancias_empleado(id_empleado) if id_empleado else None
        
        if tol_personal:
            # Usar tolerancias personalizadas
            tol_values = {
                'tolerancia_entrada': tol_personal[1],
                'tolerancia_salida_antes': tol_personal[2],
                'tolerancia_salida_despues': tol_personal[3],
                'hora_limite_extra_lj': tol_personal[4],
                'tolerancia_hora_extra': tol_personal[5],
                'horas_extra_maximo': tol_personal[6],
                'viernes_permite_extra': bool(tol_personal[7]),
                'sabado_hora_limite': tol_personal[8],
                'sabado_tolerancia': tol_personal[9],
                'sabado_entrada_minima': tol_personal[10],
                'servicios_tolerancia_entrada': tol_personal[11],
                'servicios_hora_salida_min': tol_personal[12],
                'comida_tolerancia_salida': tol_personal[14] if len(tol_personal) > 14 else 15
            }
        else:
            # Usar tolerancias globales
            tol_values = {
                'tolerancia_entrada': main.TOLERANCIA_ENTRADA_TARDE,
                'tolerancia_salida_antes': main.TOLERANCIA_SALIDA_ANTES,
                'tolerancia_salida_despues': main.TOLERANCIA_SALIDA_DESPUES,
                'hora_limite_extra_lj': main.HORA_LIMITE_EXTRA_LJ,
                'tolerancia_hora_extra': main.TOLERANCIA_HORA_EXTRA,
                'horas_extra_maximo': main.MAX_HORAS_EXTRA,
                'viernes_permite_extra': main.HORAS_EXTRA_VIERNES,
                'sabado_hora_limite': main.SABADO_HORA_LIMITE_SALIDA,
                'sabado_tolerancia': main.SABADO_TOLERANCIA_SALIDA,
                'sabado_entrada_minima': main.SABADO_ENTRADA_MINIMA_REAL,
                'servicios_tolerancia_entrada': main.SERVICIOS_TOLERANCIA_ENTRADA,
                'servicios_hora_salida_min': main.SERVICIOS_HORA_SALIDA_MIN,
                'comida_tolerancia_salida': main.COMIDA_TOLERANCIA_SALIDA
            }
        
        # Variables
        vars_dict = {}
        
        # Si es empleado individual, agregar toggles
        if id_empleado:
            sg_var = tk.BooleanVar(value=es_servicios_generales)
            sg_check = ctk.CTkCheckBox(scroll_frame, text="Empleado de Servicios Generales",
                                      variable=sg_var,
                                      font=("Segoe UI", 13),
                                      text_color=self.color_text,
                                      fg_color=self.color_accent,
                                      hover_color=self.color_accent_hover)
            sg_check.pack(anchor=tk.W, pady=(5, 5))
            
            admin_var = tk.BooleanVar(value=es_administrativo)
            admin_check = ctk.CTkCheckBox(scroll_frame, text="Administrativo (sin descuento de desayuno)",
                                         variable=admin_var,
                                         font=("Segoe UI", 13),
                                         text_color=self.color_text,
                                         fg_color=self.color_accent,
                                         hover_color=self.color_accent_hover)
            admin_check.pack(anchor=tk.W, pady=(0, 5))
            
            chofer_var = tk.BooleanVar(value=es_chofer)
            chofer_check = ctk.CTkCheckBox(scroll_frame, text="Chofer (sin salida = Viaje)",
                                          variable=chofer_var,
                                          font=("Segoe UI", 13),
                                          text_color=self.color_text,
                                          fg_color=self.color_accent,
                                          hover_color=self.color_accent_hover)
            chofer_check.pack(anchor=tk.W, pady=(0, 15))
        
        # Sección ENTRADA
        ctk.CTkLabel(scroll_frame, text="ENTRADA", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(10, 5))
        
        frame_ent = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_ent.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_ent, text="Tolerancia entrada tarde (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['tolerancia_entrada'] = ctk.CTkEntry(frame_ent, width=80)
        vars_dict['tolerancia_entrada'].insert(0, str(tol_values['tolerancia_entrada']))
        vars_dict['tolerancia_entrada'].pack(side=tk.RIGHT)
        
        # Sección ENTRADA MÍNIMA (GLOBAL para todos los días)
        ctk.CTkLabel(scroll_frame, text="ENTRADA MÍNIMA", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        frame_ent_min = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_ent_min.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_ent_min, text="Hora mínima - usar hora real antes de esto (HH:MM):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['sabado_entrada_minima'] = ctk.CTkEntry(frame_ent_min, width=80)
        vars_dict['sabado_entrada_minima'].insert(0, tol_values['sabado_entrada_minima'])
        vars_dict['sabado_entrada_minima'].pack(side=tk.RIGHT)
        
        # Sección SALIDA
        ctk.CTkLabel(scroll_frame, text="SALIDA", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        frame_sal_antes = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_sal_antes.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_sal_antes, text="Tolerancia salida antes (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['tolerancia_salida_antes'] = ctk.CTkEntry(frame_sal_antes, width=80)
        vars_dict['tolerancia_salida_antes'].insert(0, str(tol_values['tolerancia_salida_antes']))
        vars_dict['tolerancia_salida_antes'].pack(side=tk.RIGHT)
        
        frame_sal_despues = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_sal_despues.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_sal_despues, text="Tolerancia salida después (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['tolerancia_salida_despues'] = ctk.CTkEntry(frame_sal_despues, width=80)
        vars_dict['tolerancia_salida_despues'].insert(0, str(tol_values['tolerancia_salida_despues']))
        vars_dict['tolerancia_salida_despues'].pack(side=tk.RIGHT)
        
        # Sección HORAS EXTRA
        ctk.CTkLabel(scroll_frame, text="HORAS EXTRA (Lunes-Jueves)", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        frame_limite = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_limite.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_limite, text="Hora límite extra (HH:MM):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['hora_limite_extra_lj'] = ctk.CTkEntry(frame_limite, width=80)
        vars_dict['hora_limite_extra_lj'].insert(0, tol_values['hora_limite_extra_lj'])
        vars_dict['hora_limite_extra_lj'].pack(side=tk.RIGHT)
        
        frame_tol_extra = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_tol_extra.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_tol_extra, text="Tolerancia hora extra (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['tolerancia_hora_extra'] = ctk.CTkEntry(frame_tol_extra, width=80)
        vars_dict['tolerancia_hora_extra'].insert(0, str(tol_values['tolerancia_hora_extra']))
        vars_dict['tolerancia_hora_extra'].pack(side=tk.RIGHT)
        
        frame_max_extra = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_max_extra.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_max_extra, text="Máximo horas extra:",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['horas_extra_maximo'] = ctk.CTkEntry(frame_max_extra, width=80)
        vars_dict['horas_extra_maximo'].insert(0, str(tol_values['horas_extra_maximo']))
        vars_dict['horas_extra_maximo'].pack(side=tk.RIGHT)
        
        # Sección VIERNES
        ctk.CTkLabel(scroll_frame, text="VIERNES", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        vars_dict['viernes_permite_extra'] = tk.BooleanVar(value=tol_values['viernes_permite_extra'])
        ctk.CTkCheckBox(scroll_frame, text="Permitir horas extra los viernes",
                       variable=vars_dict['viernes_permite_extra'],
                       font=("Segoe UI", 12)).pack(anchor=tk.W, pady=5)
        
        # Sección SÁBADO
        ctk.CTkLabel(scroll_frame, text="SÁBADO", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        frame_sab_limite = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_sab_limite.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_sab_limite, text="Hora límite salida (HH:MM):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['sabado_hora_limite'] = ctk.CTkEntry(frame_sab_limite, width=80)
        vars_dict['sabado_hora_limite'].insert(0, tol_values['sabado_hora_limite'])
        vars_dict['sabado_hora_limite'].pack(side=tk.RIGHT)
        
        frame_sab_tol = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_sab_tol.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_sab_tol, text="Tolerancia salida (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['sabado_tolerancia'] = ctk.CTkEntry(frame_sab_tol, width=80)
        vars_dict['sabado_tolerancia'].insert(0, str(tol_values['sabado_tolerancia']))
        vars_dict['sabado_tolerancia'].pack(side=tk.RIGHT)
        
        # Sección SERVICIOS GENERALES
        ctk.CTkLabel(scroll_frame, text="SERVICIOS GENERALES", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        frame_sg_ent = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_sg_ent.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_sg_ent, text="Tolerancia entrada (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['servicios_tolerancia_entrada'] = ctk.CTkEntry(frame_sg_ent, width=80)
        vars_dict['servicios_tolerancia_entrada'].insert(0, str(tol_values['servicios_tolerancia_entrada']))
        vars_dict['servicios_tolerancia_entrada'].pack(side=tk.RIGHT)
        
        frame_sg_sal = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_sg_sal.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_sg_sal, text="Hora salida mínima (HH:MM):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['servicios_hora_salida_min'] = ctk.CTkEntry(frame_sg_sal, width=80)
        vars_dict['servicios_hora_salida_min'].insert(0, tol_values['servicios_hora_salida_min'])
        vars_dict['servicios_hora_salida_min'].pack(side=tk.RIGHT)
        
        # Sección COMIDA
        ctk.CTkLabel(scroll_frame, text="COMIDA", 
                    font=("Segoe UI", 14, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        frame_comida_tol = ctk.CTkFrame(scroll_frame, fg_color="transparent")
        frame_comida_tol.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(frame_comida_tol, text="Tolerancia salida comida (min):",
                    font=("Segoe UI", 12)).pack(side=tk.LEFT)
        vars_dict['comida_tolerancia_salida'] = ctk.CTkEntry(frame_comida_tol, width=80)
        vars_dict['comida_tolerancia_salida'].insert(0, str(tol_values['comida_tolerancia_salida']))
        vars_dict['comida_tolerancia_salida'].pack(side=tk.RIGHT)
        
        # Botones
        btn_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        btn_frame.pack(fill=tk.X, padx=20, pady=15)
        
        def guardar_config():
            """Guarda la configuración"""
            # Recopilar valores
            tol_entrada = int(vars_dict['tolerancia_entrada'].get())
            tol_sal_antes = int(vars_dict['tolerancia_salida_antes'].get())
            tol_sal_despues = int(vars_dict['tolerancia_salida_despues'].get())
            hora_limite = vars_dict['hora_limite_extra_lj'].get()
            tol_hora_extra = int(vars_dict['tolerancia_hora_extra'].get())
            max_extra = int(vars_dict['horas_extra_maximo'].get())
            viernes_extra = int(vars_dict['viernes_permite_extra'].get())
            sab_limite = vars_dict['sabado_hora_limite'].get()
            sab_tol = int(vars_dict['sabado_tolerancia'].get())
            sab_ent_min = vars_dict['sabado_entrada_minima'].get()
            sg_tol_ent = int(vars_dict['servicios_tolerancia_entrada'].get())
            sg_sal_min = vars_dict['servicios_hora_salida_min'].get()
            comida_tol = int(vars_dict['comida_tolerancia_salida'].get())
            
            if id_empleado:
                # Guardar tolerancias personalizadas
                guardar_tolerancias_empleado(
                    id_empleado, tol_entrada, tol_sal_antes, tol_sal_despues,
                    hora_limite, tol_hora_extra, max_extra, viernes_extra,
                    sab_limite, sab_tol, sab_ent_min, sg_tol_ent, sg_sal_min,
                    tolerancia_salida_minutos_minimos=30, comida_tolerancia_salida=comida_tol
                )
                
                # Detectar si los toggles especiales cambiaron
                sg_nuevo = int(sg_var.get())
                admin_nuevo = int(admin_var.get())
                chofer_nuevo = int(chofer_var.get())
                sg_cambio = es_servicios_generales != bool(sg_nuevo)
                admin_cambio = es_administrativo != bool(admin_nuevo)
                chofer_cambio = es_chofer != bool(chofer_nuevo)
                
                # Actualizar estado de Servicios Generales, Administrativo y Chofer (usar emp_data que ya tenemos)
                if emp_data:
                    guardar_empleado(
                        id_empleado, nombre_empleado, 
                        emp_data[2],  # jornada
                        emp_data[3],  # lj_ent
                        emp_data[4],  # lj_sal
                        emp_data[5],  # v_ent
                        emp_data[6],  # v_sal
                        emp_data[7],  # s_ent
                        emp_data[8],  # s_sal
                        sg_nuevo,  # es_servicios_generales
                        chofer_nuevo,  # es_chofer
                        admin_nuevo  # es_administrativo
                    )
                
                messagebox.showinfo("Éxito", f"✓ Tolerancias de {nombre_empleado} guardadas correctamente")
                self.cargar_empleados()
                
                # Si los toggles especiales cambiaron, mostrar aviso
                if sg_cambio or chofer_cambio:
                    msg = "⚠ Los cambios de Servicios Generales/Chofer han sido aplicados.\n"
                    if self.datos_asistencia_actual:
                        msg += "Regenere el reporte para ver los cambios reflejados."
                    messagebox.showinfo("Aviso", msg)
                
                dialog.destroy()
            else:
                # Actualizar valores globales en main.py
                main.TOLERANCIA_ENTRADA_TARDE = tol_entrada
                main.TOLERANCIA_SALIDA_ANTES = tol_sal_antes
                main.TOLERANCIA_SALIDA_DESPUES = tol_sal_despues
                main.HORA_LIMITE_EXTRA_LJ = hora_limite
                main.TOLERANCIA_HORA_EXTRA = tol_hora_extra
                main.MAX_HORAS_EXTRA = max_extra
                main.HORAS_EXTRA_VIERNES = bool(viernes_extra)
                main.SABADO_HORA_LIMITE_SALIDA = sab_limite
                main.SABADO_TOLERANCIA_SALIDA = sab_tol
                main.SABADO_ENTRADA_MINIMA_REAL = sab_ent_min
                main.SERVICIOS_TOLERANCIA_ENTRADA = sg_tol_ent
                main.SERVICIOS_HORA_SALIDA_MIN = sg_sal_min
                main.COMIDA_TOLERANCIA_SALIDA = comida_tol
                
                messagebox.showinfo("Éxito", "✓ Tolerancias globales actualizadas correctamente")
                dialog.destroy()
        
        def restablecer_globales():
            """Elimina tolerancias personalizadas y vuelve a usar globales"""
            if id_empleado:
                if messagebox.askyesno("Confirmar", f"¿Restablecer tolerancias globales para {nombre_empleado}?"):
                    eliminar_tolerancias_empleado(id_empleado)
                    messagebox.showinfo("Éxito", "Tolerancias restablecidas a globales")
                    dialog.destroy()
        
        btn_guardar = ctk.CTkButton(btn_frame, text="Guardar",
                                    command=guardar_config,
                                    fg_color=self.color_accent,
                                    hover_color=self.color_accent_hover,
                                    font=("Segoe UI", 12, "bold"))
        btn_guardar.pack(side=tk.LEFT, padx=5)
        
        if id_empleado:
            btn_global = ctk.CTkButton(btn_frame, text="Usar Globales",
                                      command=restablecer_globales,
                                      fg_color="#555555",
                                      hover_color="#666666",
                                      font=("Segoe UI", 12))
            btn_global.pack(side=tk.LEFT, padx=5)
        
        btn_cancelar = ctk.CTkButton(btn_frame, text="Cancelar",
                                    command=dialog.destroy,
                                    fg_color="#555555",
                                    hover_color="#666666",
                                    font=("Segoe UI", 12))
        btn_cancelar.pack(side=tk.RIGHT, padx=5)

    def guardar(self):
        if not self.id_empleado_var.get() or not self.nombre_var.get():
            messagebox.showerror("Error", "ID y Nombre son requeridos")
            return

        if not self.validar_horas():
            return

        guardar_empleado(
            self.id_empleado_var.get(),
            self.nombre_var.get(),
            self.jornada_var.get(),
            self.lj_entrada.get(),
            self.lj_salida.get(),
            self.v_entrada.get(),
            self.v_salida.get(),
            self.s_entrada.get(),
            self.s_salida.get(),
            False,  # es_servicios_generales siempre False aquí (se configura en el dialog)
            False,  # es_chofer
            False   # es_administrativo
        )

        messagebox.showinfo("Éxito", "✓ Empleado guardado correctamente")
        self.cargar_empleados()
        self.limpiar()

    def validar_horas(self):
        for hora in [self.lj_entrada.get(), self.lj_salida.get(),
                    self.v_entrada.get(), self.v_salida.get()]:
            if hora:
                h, m = hora.split(":")
                if not (0 <= int(h) <= 23 and 0 <= int(m) <= 59):
                    raise ValueError
        return True

    def limpiar(self):
        self.id_empleado_var.set("")
        self.nombre_var.set("")
        self.jornada_var.set("Lunes-Viernes")
        self._reset_horarios()
        self.s_entrada.delete(0, tk.END)
        self.s_entrada.insert(0, "08:00")
        self.s_salida.delete(0, tk.END)
        self.s_salida.insert(0, "13:00")

    def abrir_configuracion_comida(self):
        """Abre diálogo para configurar horarios de comida y desayuno"""
        import main as main_module
        
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("Configuración de Comidas")
        dialog.geometry("450x350")
        dialog.configure(fg_color=self.color_bg_main)
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Título
        titulo = ctk.CTkLabel(dialog, text="Configurar Horarios de Comidas",
                             font=("Segoe UI", 16, "bold"),
                             text_color=self.color_accent)
        titulo.pack(pady=(20, 15))
        
        # Frame de parámetros
        frame = ctk.CTkScrollableFrame(dialog, fg_color=self.color_bg_panel)
        frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # DESAYUNO
        ctk.CTkLabel(frame, text="DESAYUNO", font=("Segoe UI", 12, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(10, 5))
        
        # Hora desayuno
        h_frame = ctk.CTkFrame(frame, fg_color="transparent")
        h_frame.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(h_frame, text="Hora:", font=("Segoe UI", 11)).pack(side=tk.LEFT)
        desayuno_hora = ctk.CTkEntry(h_frame, width=100)
        desayuno_hora.insert(0, main_module.DESAYUNO_HORA)
        desayuno_hora.pack(side=tk.RIGHT)
        
        # Minutos descuento desayuno
        m_frame = ctk.CTkFrame(frame, fg_color="transparent")
        m_frame.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(m_frame, text="Minutos a descontar:", font=("Segoe UI", 11)).pack(side=tk.LEFT)
        desayuno_min = ctk.CTkEntry(m_frame, width=100)
        desayuno_min.insert(0, str(main_module.DESAYUNO_MINUTOS_DESCUENTO))
        desayuno_min.pack(side=tk.RIGHT)
        
        # COMIDA
        ctk.CTkLabel(frame, text="COMIDA", font=("Segoe UI", 12, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        # Hora comida
        c_frame = ctk.CTkFrame(frame, fg_color="transparent")
        c_frame.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(c_frame, text="Hora inicio:", font=("Segoe UI", 11)).pack(side=tk.LEFT)
        comida_hora = ctk.CTkEntry(c_frame, width=100)
        comida_hora.insert(0, main_module.COMIDA_HORA_INICIO)
        comida_hora.pack(side=tk.RIGHT)
        
        # Duración comida
        d_frame = ctk.CTkFrame(frame, fg_color="transparent")
        d_frame.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(d_frame, text="Duración (minutos):", font=("Segoe UI", 11)).pack(side=tk.LEFT)
        comida_dur = ctk.CTkEntry(d_frame, width=100)
        comida_dur.insert(0, str(main_module.COMIDA_DURACION_MINUTOS))
        comida_dur.pack(side=tk.RIGHT)
        
        # Tolerancia salida a comida
        t_frame = ctk.CTkFrame(frame, fg_color="transparent")
        t_frame.pack(fill=tk.X, pady=5)
        ctk.CTkLabel(t_frame, text="Tolerancia salida (min):", font=("Segoe UI", 11)).pack(side=tk.LEFT)
        comida_tol = ctk.CTkEntry(t_frame, width=100)
        comida_tol.insert(0, str(main_module.COMIDA_TOLERANCIA_SALIDA))
        comida_tol.pack(side=tk.RIGHT)
        
        # Frame botones
        btn_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        btn_frame.pack(fill=tk.X, padx=20, pady=15)
        
        def guardar_comida():
            main_module.DESAYUNO_HORA = desayuno_hora.get()
            main_module.DESAYUNO_MINUTOS_DESCUENTO = int(desayuno_min.get())
            main_module.COMIDA_HORA_INICIO = comida_hora.get()
            main_module.COMIDA_DURACION_MINUTOS = int(comida_dur.get())
            main_module.COMIDA_TOLERANCIA_SALIDA = int(comida_tol.get())
            
            messagebox.showinfo("Éxito", "✓ Configuración de comidas actualizada")
            dialog.destroy()
        
        btn_guardar = ctk.CTkButton(btn_frame, text="Guardar",
                                   command=guardar_comida,
                                   fg_color=self.color_accent,
                                   hover_color=self.color_accent_hover)
        btn_guardar.pack(side=tk.LEFT, padx=5)
        
        btn_cancelar = ctk.CTkButton(btn_frame, text="Cancelar",
                                    command=dialog.destroy,
                                    fg_color="#e74c3c",
                                    hover_color="#c0392b")
        btn_cancelar.pack(side=tk.LEFT, padx=5)

    def cargar_empleados(self):
        """Carga los empleados en la tabla desde la BD"""
        for item in self.tree.get_children():
            self.tree.delete(item)

        empleados = obtener_todos_empleados()
        logger.info(f"Empleados cargados de la BD: {len(empleados)}")

        for idx, emp in enumerate(empleados):
            # Manejar tanto el formato antiguo como el nuevo
            # Estructura REAL: 0-id, 1-nombre, 2-jornada, 3-8(horas), 9-fecha_registro, 10-es_servicios_generales, 11-es_chofer, 12-es_administrativo
            if len(emp) >= 13:
                id_emp, nombre, jornada, lj_ent, lj_sal, v_ent, v_sal, s_ent, s_sal, fecha_reg, es_servicios_generales, es_chofer, es_administrativo = emp[:13]
            elif len(emp) >= 12:
                # Tiene hasta es_chofer pero no es_administrativo
                id_emp, nombre, jornada, lj_ent, lj_sal, v_ent, v_sal, s_ent, s_sal, fecha_reg, es_servicios_generales, es_chofer = emp[:12]
                es_administrativo = 0
            elif len(emp) >= 11:
                # Tiene hasta es_servicios_generales pero no es_chofer
                id_emp, nombre, jornada, lj_ent, lj_sal, v_ent, v_sal, s_ent, s_sal, fecha_reg, es_servicios_generales = emp[:11]
                es_administrativo = 0
            elif len(emp) >= 10:
                # Tiene fecha_registro pero no es_servicios_generales
                id_emp, nombre, jornada, lj_ent, lj_sal, v_ent, v_sal, s_ent, s_sal, fecha_reg = emp[:10]
                es_servicios_generales = 0
                es_administrativo = 0
            else:
                # Solo los campos básicos (9 primeros)
                id_emp, nombre, jornada, lj_ent, lj_sal, v_ent, v_sal, s_ent, s_sal = emp[:9]
                es_servicios_generales = 0
                es_administrativo = 0
            
            horario_lj = f"{lj_ent}-{lj_sal}"
            horario_v = f"{v_ent}-{v_sal}"
            horario_s = f"{s_ent}-{s_sal}" if jornada == "Lunes-Sabado" else "-"

            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            # Insertar con ID como string exacto (con los ceros)
            self.tree.insert("", tk.END, values=(str(id_emp), nombre, jornada, horario_lj, horario_v, horario_s), tags=(tag,))

    def seleccionar_empleado(self, event):
        """Selecciona un empleado de la tabla y carga sus datos en el formulario"""
        item = self.tree.identify('item', event.x, event.y)

        if not item:
            return

        # Obtener valores crudos del árbol
            valores = self.tree.item(item)["values"]

            # Treeview convierte a int, necesitamos reconstruir el ID con ceros
            # Obtener el nombre (segunda columna) para buscarlo en BD
            nombre = str(valores[1])

            logger.info(f"Doble click. Valores de fila: {valores}")
            logger.info(f"Nombre: {nombre}")

            # Buscar el empleado por nombre en la BD
            todos_empleados = obtener_todos_empleados()
            emp = None
            id_emp = None

            for emp_data in todos_empleados:
                if emp_data[1] == nombre:  # emp_data[1] es el nombre
                    id_emp = emp_data[0]  # emp_data[0] es el ID
                    emp = emp_data
                    break

            logger.info(f"ID encontrado: '{id_emp}', Empleado: {emp is not None}")

            if emp:
                self._llenar_campos(emp)
                self.actualizar_campos()
            else:
                logger.warning(f"No se encontró empleado con nombre: {nombre}")

    def deseleccionar_si_vacio(self, event):
        """Deselecciona el empleado si se hace click fuera de las filas"""
        item = self.tree.identify('item', event.x, event.y)
        
        # Si no hay item en esa posición, deseleccionar
        if not item:
            self.tree.selection_remove(self.tree.selection())
            self.limpiar()

    def eliminar(self):
        """Elimina un empleado de la base de datos"""
        id_emp = self.id_empleado_var.get()
        if not id_emp:
            messagebox.showerror("Error", "Selecciona un empleado para eliminar")
            return

        confirmacion = messagebox.askyesno(
            "Confirmar eliminación",
            f"¿Estás seguro de eliminar al empleado {self.nombre_var.get()}?"
        )

        if confirmacion:
            if eliminar_empleado(id_emp):
                messagebox.showinfo("Éxito", "Empleado eliminado correctamente")
                self.cargar_empleados()
                self.limpiar()
            else:
                messagebox.showerror("Error", "No se pudo eliminar el empleado")

    def generar_reporte_detallado(self):
        """Genera el reporte detallado sin descuentos"""
        from main import generar_reporte
        # Obtener fechas del DateEntry
        fecha_inicio_obj = self.fecha_inicio_entry.get_date()
        fecha_fin_obj = self.fecha_fin_entry.get_date()
        
        # Convertir a formato yyyy-mm-dd
        fecha_inicio = fecha_inicio_obj.strftime("%Y-%m-%d")
        fecha_fin = fecha_fin_obj.strftime("%Y-%m-%d")
        
        logger.info(f"Generando reporte - Fecha inicio: {fecha_inicio}, Fecha fin: {fecha_fin}")
        
        self.status_label.configure(text="Generando reporte...", text_color="#f39c12")
        self.root.update()
        
        generar_reporte(fecha_inicio, fecha_fin)
        
        self.status_label.configure(text="✓ Reporte generado", text_color=self.color_success)
        messagebox.showinfo("Éxito", "Reporte detallado generado correctamente")

    def generar_reporte_asistencia(self):
        """Genera el reporte de asistencia"""
        from main import generar_reporte, procesar_entrada_salida, generar_reporte_asistencia_mejorado, obtener_nombres_empleados
        from requests.auth import HTTPDigestAuth
        import requests
        
        try:
            # Obtener fechas del DateEntry
            fecha_inicio_obj = self.fecha_inicio_entry.get_date()
            fecha_fin_obj = self.fecha_fin_entry.get_date()
            
            # Convertir a formato yyyy-mm-dd
            fecha_inicio = fecha_inicio_obj.strftime("%Y-%m-%d")
            fecha_fin = fecha_fin_obj.strftime("%Y-%m-%d")
            
            logger.info(f"Generando reporte de asistencia - Fecha inicio: {fecha_inicio}, Fecha fin: {fecha_fin}")
            
            self.status_label.configure(text="Generando reporte...", text_color="#f39c12")
            self.root.update()
            
            # Ejecutar generación de reporte en hilo separado para no bloquear UI
            def _generar():
                try:
                    from main import IP_RELOJ, USER, PASS, obtener_nombres_empleados
                    auth = HTTPDigestAuth(USER, PASS)
                    
                    # Obtener nombres de empleados
                    logger.info("Obteniendo nombres de empleados...")
                    nombres_map = obtener_nombres_empleados(auth)
                    
                    # Obtener eventos de asistencia (simplificado, llamando a generar_reporte)
                    import time
                    from main import generar_reporte
                    
                    # Guardar referencia temporal
                    import main
                    original_generar_excel = main.generar_excel_final
                    original_generar_asist = main.generar_reporte_asistencia
                    
                    # Desactivar generación de otros reportes temporalmente
                    datos_procesados = None
                    
                    def capturar_datos(datos):
                        nonlocal datos_procesados
                        datos_procesados = datos
                        return "temp.xlsx"
                    
                    main.generar_excel_final = capturar_datos
                    main.generar_reporte_asistencia = lambda x: None
                    
                    # Llamar a generar_reporte para obtener datos
                    generar_reporte(fecha_inicio, fecha_fin)
                    
                    # Restaurar funciones originales
                    main.generar_excel_final = original_generar_excel
                    main.generar_reporte_asistencia = original_generar_asist
                    
                    # Obtener datos procesados desde el flujo principal
                    # Por ahora, llamar directamente a procesar_entrada_salida
                    # Esta es una versión simplificada - idealmente deberías refactorizar
                    
                    self.status_label.configure(text="✓ Reporte generado", text_color=self.color_success)
                    messagebox.showinfo("Información", "Reporte de asistencia en desarrollo - próximamente")
                    
                except Exception as e:
                    logger.error(f"Error interno: {e}", exc_info=True)
                    self.status_label.configure(text="✗ Error", text_color=self.color_danger)
            
            import threading
            thread = threading.Thread(target=_generar, daemon=True)
            thread.start()
            
        except Exception as e:
            logger.error(f"Error generando reporte de asistencia: {e}", exc_info=True)
            self.status_label.configure(text="✗ Error", text_color=self.color_danger)
            messagebox.showerror("Error", f"No se pudo generar el reporte: {str(e)}")

    def generar_reporte_nomina(self):
        """Genera el reporte de nómina con horas trabajadas"""
        from datetime import datetime
        
        fecha_inicio = self.fecha_inicio_entry.get_date()
        fecha_fin = self.fecha_fin_entry.get_date()
        
        # Validar que las fechas sean viernes (weekday 4)
        if fecha_inicio.weekday() != 4:
            messagebox.showwarning("Advertencia", "La fecha de inicio debe ser un viernes")
            return
        
        if fecha_fin.weekday() != 4:
            messagebox.showwarning("Advertencia", "La fecha de fin debe ser un viernes")
            return
        
        fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
        fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")
        
        self.status_label.configure(text="Generando reporte de nómina...", text_color="#f39c12")
        self.root.update()
        
        # Ejecutar en hilo separado para no bloquear UI
        def _generar():
            try:
                from main import generar_reporte
                generar_reporte(fecha_inicio_str, fecha_fin_str)
                self.status_label.configure(text="✓ Reporte de nómina generado", text_color=self.color_success)
                messagebox.showinfo("Éxito", "Reporte de nómina generado correctamente")
            except Exception as e:
                import logging
                logger = logging.getLogger("main")
                logger.critical(f"Fallo en hilo de nómina: {type(e).__name__} - {e}", exc_info=True)
                self.status_label.configure(text="✗ Error en nómina", text_color=self.color_danger)
                messagebox.showerror("Error", f"No se pudo generar el reporte de nómina: {e}")
        
        import threading
        thread = threading.Thread(target=_generar, daemon=True)
        thread.start()

    def descargar_bd_nube(self):
        """Descarga la BD desde Firebase automáticamente"""
        from sync_cloud import cargar_configuracion_sync, sincronizar_desde_nube
        
        self.status_label.configure(text="⏳ Cargando backups disponibles...", text_color=self.color_text_dim)
        self.root.update()
        
        config = cargar_configuracion_sync()
        
        # Hacer en hilo separado
        def _descargar():
            try:
                # Obtener lista de backups disponibles
                exito, resultado = sincronizar_desde_nube(config)
                
                if not exito:
                    self.status_label.configure(text="✗ Error obteniendo backups", text_color=self.color_danger)
                    messagebox.showerror("Error", resultado)
                    return
                
                if resultado.get("tipo") == "lista":
                    backups = resultado.get("backups", [])
                    
                    if not backups:
                        messagebox.showinfo("Info", "No hay backups disponibles aún.\n\nHaz clic en 'Subir a Nube' para crear el primero.")
                        self.status_label.configure(text="ℹ No hay backups", text_color=self.color_text_dim)
                        return
                    
                    # Mostrar diálogo para seleccionar backup
                    dialog = ctk.CTkToplevel(self.root)
                    dialog.title("Descargar Base de Datos")
                    dialog.geometry("450x350")
                    dialog.resizable(False, False)
                    dialog.transient(self.root)  # Asociar con ventana principal
                    dialog.grab_set()  # Hacer modal (bloquea ventana principal)
                    dialog.lift()  # Traer al frente
                    dialog.focus()  # Dar foco
                    dialog.attributes('-topmost', True)  # Mantener siempre arriba
                    
                    # Título
                    ctk.CTkLabel(dialog, text="📊 Backups Disponibles",
                                font=("Segoe UI", 14, "bold"),
                                text_color=self.color_accent).pack(pady=10)
                    
                    ctk.CTkLabel(dialog, text="Selecciona cuál descargar:",
                                font=("Segoe UI", 11),
                                text_color=self.color_text_dim).pack(pady=(0, 10))
                    
                    # Listbox de backups
                    frame_list = ctk.CTkFrame(dialog)
                    frame_list.pack(fill=tk.BOTH, expand=True, padx=15, pady=5)
                    
                    listbox = tk.Listbox(frame_list, height=10, width=50, font=("Courier", 10),
                                        bg="#2a2a2a", fg="#ffffff", selectmode=tk.SINGLE)
                    listbox.pack(fill=tk.BOTH, expand=True)
                    
                    # Añadir backups con índice
                    for idx, backup_info in enumerate(backups):
                        try:
                            # backup_info puede ser string (timestamp) o dict con metadata
                            if isinstance(backup_info, dict):
                                backup = backup_info.get('timestamp', '')
                                usuario = backup_info.get('usuario', 'Desconocido')
                            else:
                                backup = backup_info
                                usuario = 'N/A'
                            
                            # Parsear timestamp para mostrar más legible
                            # Formato: 20260109_150000 -> 2026-01-09 15:00:00
                            fecha_str = f"{backup[:4]}-{backup[4:6]}-{backup[6:8]} {backup[9:11]}:{backup[11:13]}:{backup[13:15]}"
                            es_mas_reciente = " (Más reciente)" if idx == 0 else ""
                            usuario_str = f" - {usuario}" if usuario != 'N/A' else ""
                            listbox.insert(tk.END, f"{fecha_str}{usuario_str}{es_mas_reciente}")
                        except:
                            listbox.insert(tk.END, backup)
                    
                    # Seleccionar el primero (más reciente) por defecto
                    if backups:
                        listbox.selection_set(0)
                    
                    # Info de selección
                    info_label = ctk.CTkLabel(dialog, text="", font=("Segoe UI", 9),
                                             text_color=self.color_text_dim)
                    info_label.pack(pady=5)
                    
                    def _actualizar_info():
                        selection = listbox.curselection()
                        if selection:
                            info_label.configure(text=f"Opción {selection[0] + 1} de {len(backups)}")
                    
                    listbox.bind('<<ListboxSelect>>', lambda e: _actualizar_info())
                    _actualizar_info()
                    
                    # Botones
                    btn_frame = ctk.CTkFrame(dialog, fg_color="transparent")
                    btn_frame.pack(fill=tk.X, padx=15, pady=15)
                    
                    def _descargar_seleccionado():
                        selection = listbox.curselection()
                        if not selection:
                            messagebox.showwarning("Advertencia", "Selecciona un backup")
                            return
                        
                        # Extraer timestamp del backup seleccionado
                        backup_info = backups[selection[0]]
                        if isinstance(backup_info, dict):
                            timestamp = backup_info.get('timestamp')
                        else:
                            timestamp = backup_info
                        
                        dialog.destroy()
                        
                        # Descargar backup específico
                        self.status_label.configure(text=f"⏳ Descargando backup...", text_color=self.color_text_dim)
                        self.root.update()
                        
                        exito, mensaje = sincronizar_desde_nube(config, timestamp=timestamp)
                        if exito:
                            self.status_label.configure(text="✓ Base de datos descargada", text_color=self.color_success)
                            messagebox.showinfo("Éxito", "✓ Base de datos descargada e importada correctamente")
                            self.cargar_empleados()
                        else:
                            self.status_label.configure(text="✗ Error descargando", text_color=self.color_danger)
                            messagebox.showerror("Error", f"Error: {mensaje}")
                    
                    btn_descargar = ctk.CTkButton(btn_frame, text="✓ Descargar",
                                                 command=_descargar_seleccionado,
                                                 fg_color=self.color_success,
                                                 hover_color="#27ae60",
                                                 font=("Segoe UI", 12, "bold"))
                    btn_descargar.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
                    
                    btn_cancelar = ctk.CTkButton(btn_frame, text="✗ Cancelar",
                                                command=dialog.destroy,
                                                fg_color="#e74c3c",
                                                hover_color="#c0392b",
                                                font=("Segoe UI", 12, "bold"))
                    btn_cancelar.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
                    
                    self.status_label.configure(text="✓ Backups cargados", text_color=self.color_success)
                else:
                    messagebox.showerror("Error", "Respuesta inesperada del servidor")
            except Exception as e:
                logger.error(f"Error descargando BD: {e}")
                self.status_label.configure(text="✗ Error", text_color=self.color_danger)
                messagebox.showerror("Error", f"Error: {e}")
        
        import threading
        thread = threading.Thread(target=_descargar, daemon=True)
        thread.start()

    def subir_bd_nube(self):
        """Sube la BD a Firebase automáticamente"""
        from sync_cloud import cargar_configuracion_sync, sincronizar_a_nube
        from tkinter import simpledialog
        
        # Pedir nombre del usuario
        nombre_usuario = simpledialog.askstring(
            "Subir Base de Datos",
            "¿Quién sube esta base de datos?\n\n(Nombre completo o iniciales):",
            parent=self.root
        )
        
        if not nombre_usuario or nombre_usuario.strip() == "":
            messagebox.showwarning("Cancelado", "Se canceló la subida. Debe proporcionar un nombre.")
            return
        
        nombre_usuario = nombre_usuario.strip()
        config = cargar_configuracion_sync()
        
        self.status_label.configure(text="⏳ Subiendo base de datos...", text_color=self.color_text_dim)
        self.root.update()
        
        def _subir():
            try:
                exito, mensaje = sincronizar_a_nube(config, nombre_usuario=nombre_usuario)
                if exito:
                    self.status_label.configure(text="✓ Base de datos guardada", text_color=self.color_success)
                    messagebox.showinfo("Éxito", f"✓ Base de datos guardada en la nube\n\nSubida por: {nombre_usuario}\n{mensaje}")
                else:
                    self.status_label.configure(text="✗ Error subiendo", text_color=self.color_danger)
                    messagebox.showerror("Error", f"Error: {mensaje}")
            except Exception as e:
                logger.error(f"Error subiendo BD: {e}")
                self.status_label.configure(text="✗ Error", text_color=self.color_danger)
                messagebox.showerror("Error", f"Error: {e}")
        
        import threading
        thread = threading.Thread(target=_subir, daemon=True)
        thread.start()

    def gestionar_backups(self):
        """Abre ventana para gestionar backups locales"""
        from sync_cloud import BACKUP_DIR
        import os
        
        if not os.path.exists(BACKUP_DIR) or not os.listdir(BACKUP_DIR):
            messagebox.showinfo("Backups", "No hay backups locales disponibles.")
            return
        
        # Abrir carpeta de backups
        import subprocess
        import platform
        
        if platform.system() == "Windows":
            os.startfile(BACKUP_DIR)
        elif platform.system() == "Darwin":  # macOS
            subprocess.Popen(["open", BACKUP_DIR])
        else:  # Linux
            subprocess.Popen(["xdg-open", BACKUP_DIR])
        
        messagebox.showinfo("Backups", f"Carpeta de backups abierta:\n{BACKUP_DIR}")

    def _configurar_nube(self):
        """Abre diálogo para configurar sincronización en la nube"""
        from sync_cloud import cargar_configuracion_sync, guardar_configuracion_sync
        
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("Configurar Sincronización en la Nube")
        dialog.geometry("500x400")
        dialog.resizable(False, False)
        
        # Centro en pantalla
        dialog.transient(self.root)
        dialog.grab_set()
        
        config = cargar_configuracion_sync()
        
        # Frame scroll
        scroll_frame = ctk.CTkScrollableFrame(dialog, fg_color=self.color_bg_input)
        scroll_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)
        
        # Título
        ctk.CTkLabel(scroll_frame, text="Selecciona el servicio de nube", 
                    font=("Segoe UI", 13, "bold"),
                    text_color=self.color_text).pack(anchor=tk.W, pady=(0, 10))
        
        # Variable para el backend
        backend_var = tk.StringVar(value=config.get("backend", "local"))
        
        # Opciones de backend
        backends = [
            ("Local (Backups)", "local"),
            ("jsonbin.io", "jsonbin"),
            ("GitHub", "github")
        ]
        
        for label, value in backends:
            ctk.CTkRadioButton(scroll_frame, text=label, variable=backend_var, value=value,
                             font=("Segoe UI", 12), text_color=self.color_text).pack(anchor=tk.W, pady=5)
        
        # Frame para credenciales
        ctk.CTkLabel(scroll_frame, text="Credenciales (opcional)", 
                    font=("Segoe UI", 12, "bold"),
                    text_color=self.color_accent).pack(anchor=tk.W, pady=(15, 5))
        
        ctk.CTkLabel(scroll_frame, text="API Key / Token:", 
                    font=("Segoe UI", 11), text_color=self.color_text).pack(anchor=tk.W)
        api_key_entry = ctk.CTkEntry(scroll_frame, placeholder_text="Ingresa tu API Key")
        api_key_entry.insert(0, config.get("api_key", ""))
        api_key_entry.pack(fill=tk.X, pady=5)
        
        ctk.CTkLabel(scroll_frame, text="Repositorio / Bin ID:", 
                    font=("Segoe UI", 11), text_color=self.color_text).pack(anchor=tk.W)
        url_entry = ctk.CTkEntry(scroll_frame, placeholder_text="usuario/repo o bin_id")
        url_entry.insert(0, config.get("url", "") or config.get("bucket_id", ""))
        url_entry.pack(fill=tk.X, pady=5)
        
        # Info
        info_text = """Servicios recomendados:
        
• jsonbin.io: Gratis, solo necesitas API Key
• GitHub: Gratis, necesitas token + usuario/repo
• Local: Solo backups en tu computadora"""
        
        ctk.CTkLabel(scroll_frame, text=info_text, 
                    font=("Segoe UI", 10), text_color=self.color_text_dim,
                    justify=tk.LEFT).pack(anchor=tk.W, pady=(15, 0))
        
        # Botones
        btn_frame = ctk.CTkFrame(dialog, fg_color="transparent")
        btn_frame.pack(fill=tk.X, padx=15, pady=15)
        
        def guardar_config():
            nueva_config = {
                "backend": backend_var.get(),
                "api_key": api_key_entry.get(),
                "bucket_id": "",
                "url": url_entry.get()
            }
            guardar_configuracion_sync(nueva_config)
            messagebox.showinfo("Éxito", "Configuración guardada correctamente")
            dialog.destroy()
        
        btn_guardar = ctk.CTkButton(btn_frame, text="Guardar", command=guardar_config,
                                   fg_color=self.color_accent, hover_color=self.color_accent_hover)
        btn_guardar.pack(side=tk.LEFT, padx=5)
        
        btn_cancelar = ctk.CTkButton(btn_frame, text="Cancelar", command=dialog.destroy,
                                    fg_color="#e74c3c", hover_color="#c0392b")
        btn_cancelar.pack(side=tk.LEFT, padx=5)


def main():
    root = ctk.CTk()
    app = GUIJornadaLaboral(root)
    root.mainloop()


if __name__ == "__main__":
    main()
