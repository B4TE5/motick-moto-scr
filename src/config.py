"""
================================================================================
                          CONFIGURACION CORREGIDA                   
================================================================================

Autor: Carlos Peraza
Versión: 2.0
Fecha: Agosto 2025

================================================================================
"""

import os
from datetime import datetime
from typing import List  # ← IMPORT AGREGADO PARA SOLUCIONAR EL ERROR

# ============================================================================
# CONFIGURACION DE MODELOS DE MOTOS (Sin cambios)
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
        "sheet_name": "Honda_CB125R",  # Será reformateado automáticamente
        "keywords": ["honda cb125r", "honda cb 125 r", "cb125r", "cb 125 r"],
        "exclude_keywords": ["cb125f", "cb250r", "cb500r", "cb650r"],
        "cron_schedule": "0 6 * * *"
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
        "cron_schedule": "0 7 * * *"
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
        "cron_schedule": "0 8 * * *"
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
        "cron_schedule": "0 10 * * *"
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
        "cron_schedule": "0 11 * * *"
    }
}

# ============================================================================
# CONFIGURACION DE GOOGLE SHEETS (Sin cambios)
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
# CONFIGURACION DE RENTABILIDAD (Sin cambios)
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
# CONFIGURACION DE LIMPIEZA DE DATOS (Sin cambios)
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
        'garantia', 'taller', 'motor', 'automocion', 'vehiculos', 'mundimoto',
        's.l.', 'sl', 's.a.', 'sa', 'sociedad', 'cia', 'ltd'
    ]
}

# ============================================================================
# CONFIGURACION DE SELENIUM CORREGIDA - TIMEOUTS EXTENDIDOS
# ============================================================================

SELENIUM_CONFIG = {
    # TIMEOUTS EXTENDIDOS PARA MAS TIEMPO DE SCRAPING
    "implicit_wait": 2.0,           # Aumentado de 0.5 a 2.0
    "page_load_timeout": 30,        # Aumentado de 15 a 30
    "script_timeout": 20,           # Aumentado de 10 a 20
    
    # REINTENTOS PARA MAYOR ROBUSTEZ  
    "max_retries": 5,               # Aumentado de 3 a 5
    "retry_delay": 3,               # Aumentado de 2 a 3
    
    # SCROLL Y CARGA DE CONTENIDO
    "scroll_pause": 2.5,            # Aumentado de 1.5 a 2.5
    "max_scroll_attempts": 10,      # Aumentado de 5 a 10
    
    # CONFIGURACION DE NAVEGADOR
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    
    # TIMEOUTS ESPECIFICOS POR ENTORNO
    "github_actions_timeouts": {
        "max_total_scraping_time": 14400,      # 4 horas para GitHub Actions
        "max_time_per_url": 2400,              # 40 minutos por URL
        "max_anuncios_per_url": 50,            # 50 anuncios por URL
        "pause_between_urls": 2,               # 2 segundos entre URLs
        "pause_between_anuncios": 1            # 1 segundo entre anuncios
    },
    
    "local_timeouts": {
        "max_total_scraping_time": 18000,      # 5 horas para local
        "max_time_per_url": 3600,              # 1 hora por URL  
        "max_anuncios_per_url": 999999,        # Sin límite local
        "pause_between_urls": 3,               # 3 segundos entre URLs
        "pause_between_anuncios": 1.5          # 1.5 segundos entre anuncios
    }
}

# ============================================================================
# CONFIGURACION DE LOGGING MEJORADA
# ============================================================================

LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_file": f"logs/scraper_{datetime.now().strftime('%Y%m%d')}.log",
    "max_log_size": 50 * 1024 * 1024,  # Aumentado a 50MB
    "backup_count": 10                   # Aumentado a 10 backups
}

# ============================================================================
# CONFIGURACION DE ARCHIVOS DE RESULTADOS
# ============================================================================

RESULTADOS_CONFIG = {
    "directorio": "resultados",
    "formato_nombre": "{modelo}_{timestamp}.xlsx",
    "formato_timestamp": "%Y%m%d_%H%M%S",
    "incluir_estadisticas": True,
    "incluir_raw_data": False,
    
    # CONFIGURACION DE COLUMNAS PARA GOOGLE SHEETS
    "columnas_ordenadas": [
        "Título",
        "Precio", 
        "Kilometraje",
        "Año",
        "Rentabilidad",           # Categoría simple, no score numérico
        "Vendedor",
        "Ubicación", 
        "Fecha_Publicacion",
        "URL",
        "Fecha_Extraccion"
    ],
    
    "columnas_excluidas": [
        "Rentabilidad_Score",     # No mostrar score numérico
        "Ranking_Rentabilidad",   # No mostrar ranking
        "Descripcion",            # No mostrar descripción
        "Categoria_Rentabilidad"  # No mostrar categoría detallada
    ]
}

# ============================================================================
# NUEVA CONFIGURACION PARA GENERACION DE URLs
# ============================================================================

URL_GENERATION_CONFIG = {
    # REGIONES PRINCIPALES PARA BUSQUEDAS GEOGRAFICAS
    "regiones_principales": [
        ("madrid", "40.4168", "-3.7038"),
        ("barcelona", "41.3851", "2.1734"),
        ("valencia", "39.4699", "-0.3763"),
        ("sevilla", "37.3891", "-5.9845"),
        ("bilbao", "43.2627", "-2.9253"),
        ("zaragoza", "41.6488", "-0.8891"),
        ("malaga", "36.7196", "-4.4214"),
        ("murcia", "37.9922", "-1.1307"),
        ("palma", "39.5696", "2.6502"),
        ("las palmas", "28.1248", "-15.4300")
    ],
    
    # TIPOS DE ORDENAMIENTO
    "ordenamientos": [
        "",  # Sin ordenamiento
        "&order_by=newest",
        "&order_by=price_low_to_high", 
        "&order_by=price_high_to_low",
        "&order_by=closest"
    ],
    
    # AÑOS PARA BUSQUEDAS ESPECIFICAS
    "años_busqueda": [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
    
    # TERMINOS ADICIONALES POR TIPO DE MOTO
    "terminos_por_tipo": {
        "125cc": ["moto 125", "125cc", "carnet a1", "deportiva 125"],
        "scooter": ["scooter", "automatico", "urbano", "ciudad"],
        "naked": ["naked", "sport", "deportiva", "carretera"],
        "premium": ["seminueva", "pocos km", "como nueva", "poco uso"]
    }
}

# ============================================================================
# FUNCIONES HELPER (Sin cambios excepto timeouts)
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

def get_timeout_config():
    """Obtiene configuración de timeouts según el entorno"""
    if is_github_actions():
        return SELENIUM_CONFIG["github_actions_timeouts"]
    else:
        return SELENIUM_CONFIG["local_timeouts"]

def get_log_level():
    """Obtiene el nivel de logging configurado"""
    return os.getenv("LOG_LEVEL", LOGGING_CONFIG["level"])

# ============================================================================
# VALIDACION DE CONFIGURACION MEJORADA
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
    
    # Validar timeouts
    timeout_config = get_timeout_config()
    if timeout_config["max_total_scraping_time"] <= 0:
        errors.append("max_total_scraping_time debe ser mayor a 0")
    
    if errors:
        raise ValueError("Errores en configuración:\n" + "\n".join(errors))
    
    return True

# ============================================================================
# NUEVA FUNCION PARA GENERAR URLs MASIVAS
# ============================================================================

def generate_extended_urls(modelo_key: str, base_queries: List[str]) -> List[str]:
    """
    Generar URLs extendidas para un modelo específico
    
    Args:
        modelo_key: Clave del modelo
        base_queries: Consultas base específicas del modelo
        
    Returns:
        Lista extendida de URLs de búsqueda
    """
    config = get_modelo_config(modelo_key)
    url_config = URL_GENERATION_CONFIG
    
    urls = []
    min_price = config['precio_min']
    max_price = config['precio_max']
    
    # URLs básicas con todos los ordenamientos
    for query in base_queries:
        for ordering in url_config["ordenamientos"]:
            url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}{ordering}"
            urls.append(url)
    
    # URLs por regiones
    for region, lat, lon in url_config["regiones_principales"]:
        for query in base_queries[:3]:  # Solo las 3 consultas principales por región
            for ordering in url_config["ordenamientos"][:3]:  # Solo 3 ordenamientos por región
                url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&latitude={lat}&longitude={lon}&distance=50000{ordering}"
                urls.append(url)
    
    # URLs por años específicos
    for year in url_config["años_busqueda"]:
        if config['año_min'] <= year <= config['año_max']:
            for query in base_queries[:2]:  # Solo las 2 consultas principales por año
                year_query = f"{query}%20{year}"
                url = f"https://es.wallapop.com/app/search?keywords={year_query}&min_sale_price={min_price}&max_sale_price={max_price}"
                urls.append(url)
    
    # Eliminar duplicados
    unique_urls = list(dict.fromkeys(urls))
    
    return unique_urls

if __name__ == "__main__":
    # Validar configuración al importar
    try:
        validate_config()
        print("Configuración válida - TIMEOUTS EXTENDIDOS APLICADOS")
        print(f"Modelos configurados: {len(MODELOS_CONFIG)}")
        
        timeout_config = get_timeout_config()
        print(f"Tiempo máximo de scraping: {timeout_config['max_total_scraping_time']/3600:.1f} horas")
        print(f"Tiempo máximo por URL: {timeout_config['max_time_per_url']/60:.1f} minutos")
        
        for modelo in get_all_modelos():
            config = get_modelo_config(modelo)
            print(f"   • {config['nombre']} ({config['tipo']})")
            
    except ValueError as e:
        print(f"Error en configuración: {e}")
