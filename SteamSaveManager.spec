# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_all


ROOT = Path(__file__).resolve().parent

if (ROOT / "main.py").is_file():
    ENTRY_SCRIPT = "main.py"
elif (ROOT / "steam_save_manager.py").is_file():
    ENTRY_SCRIPT = "steam_save_manager.py"
else:
    raise FileNotFoundError("Expected main.py or steam_save_manager.py in project root.")


datas = []
binaries = []
hiddenimports = [
    "pystray._win32",
    "pystray",
    "PIL",
    "PIL.Image",
    "psutil",
    "watchdog",
    "watchdog.observers",
    "watchdog.events",
]

for package_name in ("customtkinter", "pystray"):
    collected = collect_all(package_name)
    datas += collected[0]
    binaries += collected[1]
    hiddenimports += collected[2]


a = Analysis(
    [ENTRY_SCRIPT],
    pathex=[str(ROOT)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["PIL._avif"],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="SteamSaveManager",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon="NONE",
)
