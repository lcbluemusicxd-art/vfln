"""
Build Windows executable for VAFERLU Jornada Laboral using PyInstaller.
- Generates branded assets (PNG + ICO)
- Builds a one-file executable with icon and version

Usage:
  py -m pip install -r requirements.txt
  py build_windows.py

Result:
  dist/vaferlu-jornada/ (folder) or single exe if onefile
"""
import os
import subprocess
import sys
import shutil

ROOT = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

# Ensure assets exist via Pillow generator
print("[1/3] Generating brand assets...")
try:
    subprocess.check_call([PY, os.path.join(ROOT, 'tools', 'generate_brand_assets.py')])
except subprocess.CalledProcessError as e:
    print("Failed to generate brand assets:", e)
    sys.exit(1)

print("[2/3] Building with PyInstaller (onefile)...")
# App metadata
app_name = 'VAFERLU-Jornada-Laboral'
entry = os.path.join(ROOT, 'gui_jornada.py')
icon = os.path.join(ROOT, 'assets', 'vaferlu.ico')
version_file = os.path.join(ROOT, 'VERSION')
version = '3.0.0'
if os.path.exists(version_file):
    with open(version_file, 'r', encoding='utf-8') as f:
        version = f.read().strip()

# Bundle data (assets for icon at runtime)
add_data = f"{os.path.join(ROOT, 'assets')}{os.pathsep}assets"

# Excluir base de datos y archivos de configuración de la compilación
exclude_files = [
    'jornada_laboral.db',
    'sync_config.json',
    'historial_sync.json',
    '*.db',
    'backups_cloud/*'
]

# Clean previous build artifacts to avoid stale outputs
for folder in ["build", "dist"]:
    target = os.path.join(ROOT, folder)
    if os.path.isdir(target):
        shutil.rmtree(target)

cmd = [
    PY, '-m', 'PyInstaller',
    '--noconfirm',
    '--clean',
    '--name', app_name,
    '--onefile',  # single EXE, incluye runtime de Python
    '--windowed',
    f"--add-data={add_data}",
    # Excluir archivos de base de datos y configuración
    '--exclude-module', 'jornada_laboral.db',
]
if os.path.exists(icon):
    cmd.append(f"--icon={icon}")
cmd.append(entry)

print('Command:', ' '.join(cmd))
subprocess.check_call(cmd)

print("[3/3] Build complete. Archivo único en dist/.")
