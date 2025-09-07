"""
================================================================================
                    CONFIGURACIÓN GENERAL - WALLAPOP MOTOS SCRAPER                    
================================================================================

Configuración centralizada para el sistema de scraping de motos
Incluye modelos, URLs, criterios y configuraciones de Google Sheets

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

import os
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN DE MODELOS DE MOTOS
# ============================================================================

MODELOS_CONFIG = {
    "cb125r": {
        "nombre": "Honda CB125R",
        "marca": "Honda",
        "tipo": "Deportiva 125cc",
        "precio_min": 1000,
        "precio_max": 4500,
        "km_max": 25000,
        "año_min": 2018,
        "año_max": 2025,
        "sheet_name": "Honda_CB125R",
        "keywords": ["honda cb125r", "honda cb 125 r", "cb125r", "cb 125 r"],
        "exclude_keywords": ["cb125f", "cb250r", "cb500r", "cb650r"],
        "cron_schedule": "0 6 * * *"  # 06:00 UTC
    },
    "pcx125": {
        "nombre": "Honda PCX125",
        "marca": "Honda", 
        "tipo": "Scooter urbano",
        "precio_min": 1200,
        "precio_max": 4000,
        "km_max": 30000,
        "año_min": 2016,
        "año_max": 2025,
        "sheet_name": "Honda_PCX125",
        "keywords": ["honda pcx125", "honda pcx 125", "pcx125", "pcx 125"],
        "exclude_keywords": ["pcx150", "pcx160", "forza"],
        "cron_schedule": "0 7 * * *"  # 07:00 UTC
    },
    "agility125": {
        "nombre": "Kymco Agility125",
        "marca": "Kymco",
        "tipo": "Scooter económico",
        "precio_min": 800,
        "precio_max": 3000,
        "km_max": 35000,
        "año_min": 2014,
        "año_max": 2025,
        "sheet_name": "Kymco_Agility125",
        "keywords": ["kymco agility125", "kymco agility 125", "agility125", "agility 125"],
        "exclude_keywords": ["agility50", "agility16", "agility200", "people"],
        "cron_schedule": "0 8 * * *"  # 08:00 UTC
    },
    "z800": {
        "nombre": "Kawasaki Z800",
        "marca": "Kawasaki",
        "tipo": "Naked media cilindrada",
        "precio_min": 3500,
        "precio_max": 7000,
        "km_max": 50000,
        "año_min": 2013,
        "año_max": 2016,
        "sheet_name": "Kawasaki_Z800",
        "keywords": ["kawasaki z800", "kawasaki z 800", "z800", "z 800"],
        "exclude_keywords": ["z900", "z650", "z1000", "z300"],
        "cron_schedule": "0 9 * * *"  # 09:00 UTC
    },
    "z900": {
        "nombre": "Kawasaki Z900",
        "marca": "Kawasaki",
        "tipo": "Naked alta cilindrada",
        "precio_min": 4500,
        "precio_max": 9000,
        "km_max": 30000,
        "año_min": 2017,
        "año_max": 2025,
        "sheet_name": "Kawasaki_Z900",
        "keywords": ["kawasaki z900", "kawasaki z 900", "z900", "z 900"],
        "exclude_keywords": ["z800", "z650", "z1000", "z300"],
        "cron_schedule": "0 10 * * *"  # 10:00 UTC
    },
    "mt07": {
        "nombre": "Yamaha MT-07",
        "marca": "Yamaha",
        "tipo": "Naked versátil",
        "precio_min": 3000,
        "precio_max": 7500,
        "km_max": 40000,
        "año_min": 2014,
        "año_max": 2025,
        "sheet_name": "Yamaha_MT07",
        "keywords": ["yamaha mt07", "yamaha mt-07", "yamaha mt 07", "mt07", "mt-07"],
        "exclude_keywords": ["mt03", "mt09", "mt10", "mt125"],
        "cron_schedule": "0 11 * * *"  # 11:00 UTC
    },
    "xmax125": {
        "nombre": "Yamaha XMAX125",
        "marca": "Yamaha",
        "tipo": "Maxiscooter premium",
        "precio_min": 1500,
        "precio_max": 4800,
        "km_max": 25000,
        "año_min": 2017,
        "año_max": 2025,
        "sheet_name": "Yamaha_XMAX125",
        "keywords": ["yamaha xmax125", "yamaha xmax 125", "yamaha x-max 125", "xmax125"],
        "exclude_keywords": ["xmax250", "xmax300", "xmax400", "tmax"],
        "cron_schedule": "0 12 * * *"  # 12:00 UTC
    }
}

# ============================================================================
# CONFIGURACIÓN DE GOOGLE SHEETS
# ============================================================================

GOOGLE_SHEETS_CONFIG = {
    "scopes": [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ],
    "credentials_env_var": "GOOGLE_CREDENTIALS_JSON",
    "sheet_id_env_var": "GOOGLE_SHEET_ID",
    "local_credentials_file": "credentials/service-account.json"
}

# ============================================================================
# CONFIGURACIÓN DE RENTABILIDAD
# ============================================================================

RENTABILIDAD_CONFIG = {
    "pesos": {
        "precio": 0.40,      # 40% - Precio más bajo = mejor
        "kilometraje": 0.35, # 35% - Menos km = mejor  
        "año": 0.25          # 25% - Más reciente = mejor
    },
    "normalizacion": {
        "precio_max_score": 10,    # Puntuación máxima para precio
        "km_max_score": 10,        # Puntuación máxima para km
        "año_max_score": 10        # Puntuación máxima para año
    }
}

# ============================================================================
# CONFIGURACIÓN DE LIMPIEZA DE DATOS
# ============================================================================

LIMPIEZA_CONFIG = {
    "filtros": {
        "precio_minimo_global": 500,    # Precio mínimo absoluto
        "km_maximo_global": 80000,      # KM máximo absoluto
        "eliminar_comerciales": True,   # Eliminar anuncios comerciales
        "eliminar_islas": True,         # Eliminar anuncios de islas
        "eliminar_duplicados": True     # Eliminar duplicados por URL
    },
    "islas_spain": [
        'canarias', 'tenerife', 'gran canaria', 'fuerteventura', 'lanzarote',
        'la palma', 'la gomera', 'el hierro', 'baleares', 'mallorca', 'menorca', 
        'ibiza', 'formentera', 'palma', 'ceuta', 'melilla', 'palma de mallorca'
    ],
    "comerciales_keywords": [
        'concesionario', 'profesional', 'empresa', 'dealer', 'financiacion',
        'garantia', 'taller', 'motor', 'automocion', 'vehiculos'
    ]
}

# ============================================================================
# CONFIGURACIÓN DE SELENIUM
# ============================================================================

SELENIUM_CONFIG = {
    "implicit_wait": 0.5,
    "page_load_timeout": 15,
    "script_timeout": 10,
    "max_retries": 3,
    "retry_delay": 2,
    "scroll_pause": 1.5,
    "max_scroll_attempts": 5,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================

LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_file": f"logs/scraper_{datetime.now().strftime('%Y%m%d')}.log",
    "max_log_size": 10 * 1024 * 1024,  # 10MB
    "backup_count": 5
}

# ============================================================================
# CONFIGURACIÓN DE ARCHIVOS DE RESULTADOS
# ============================================================================

RESULTADOS_CONFIG = {
    "directorio": "resultados",
    "formato_nombre": "{modelo}_{timestamp}.xlsx",
    "formato_timestamp": "%Y%m%d_%H%M%S",
    "sheets_por_año": True,
    "incluir_estadisticas": True,
    "incluir_raw_data": False
}

# ============================================================================
# FUNCIONES HELPER
# ============================================================================

def get_modelo_config(modelo_key):
    """Obtiene la configuración de un modelo específico"""
    if modelo_key not in MODELOS_CONFIG:
        raise ValueError(f"Modelo '{modelo_key}' no encontrado en configuración")
    return MODELOS_CONFIG[modelo_key]

def get_all_modelos():
    """Obtiene lista de todos los modelos disponibles"""
    return list(MODELOS_CONFIG.keys())

def get_google_credentials_path():
    """Obtiene la ruta de las credenciales de Google"""
    # Prioridad: variable de entorno -> archivo local
    if os.getenv(GOOGLE_SHEETS_CONFIG["credentials_env_var"]):
        return "ENV"
    elif os.path.exists(GOOGLE_SHEETS_CONFIG["local_credentials_file"]):
        return GOOGLE_SHEETS_CONFIG["local_credentials_file"]
    else:
        return None

def get_sheet_id():
    """Obtiene el ID del Google Sheet"""
    return os.getenv(GOOGLE_SHEETS_CONFIG["sheet_id_env_var"])

def is_github_actions():
    """Detecta si se está ejecutando en GitHub Actions"""
    return os.getenv("GITHUB_ACTIONS") == "true"

def get_log_level():
    """Obtiene el nivel de logging configurado"""
    return os.getenv("LOG_LEVEL", LOGGING_CONFIG["level"])

# ============================================================================
# VALIDACIÓN DE CONFIGURACIÓN
# ============================================================================

def validate_config():
    """Valida que la configuración sea correcta"""
    errors = []
    
    # Validar modelos
    for modelo, config in MODELOS_CONFIG.items():
        required_fields = ["nombre", "marca", "precio_min", "precio_max", "sheet_name"]
        for field in required_fields:
            if field not in config:
                errors.append(f"Modelo {modelo}: falta campo '{field}'")
        
        if config.get("precio_min", 0) >= config.get("precio_max", 0):
            errors.append(f"Modelo {modelo}: precio_min debe ser menor que precio_max")
    
    # Validar pesos de rentabilidad
    total_peso = sum(RENTABILIDAD_CONFIG["pesos"].values())
    if abs(total_peso - 1.0) > 0.01:
        errors.append(f"Pesos de rentabilidad deben sumar 1.0, actual: {total_peso}")
    
    if errors:
        raise ValueError("Errores en configuración:\n" + "\n".join(errors))
    
    return True

if __name__ == "__main__":
    # Validar configuración al importar
    try:
        validate_config()
        print(" Configuración válida")
        print(f" Modelos configurados: {len(MODELOS_CONFIG)}")
        for modelo in get_all_modelos():
            config = get_modelo_config(modelo)
            print(f"   • {config['nombre']} ({config['tipo']})")
    except ValueError as e:
        print(f" Error en configuración: {e}")