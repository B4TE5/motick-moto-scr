"""
================================================================================
                    SCRAPERS MODULE - WALLAPOP MOTOS SCRAPER                    
================================================================================

Módulo de scrapers específicos para cada modelo de moto
Exporta todos los scrapers disponibles para uso fácil

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

# Importar scraper base
from .base_scraper import BaseScraper

# Importar todos los scrapers específicos
from .scraper_cb125r import ScraperCB125R
from .scraper_pcx125 import ScraperPCX125
from .scraper_agility125 import ScraperAGILITY125
from .scraper_z900 import ScraperZ900
from .scraper_mt07 import ScraperMT07

# Mapeo de modelos a clases de scrapers
SCRAPER_CLASSES = {
    'cb125r': ScraperCB125R,
    'pcx125': ScraperPCX125,
    'agility125': ScraperAGILITY125,
    'z900': ScraperZ900,
    'mt07': ScraperMT07
}

# Funciones de ejecución directa
from .scraper_cb125r import run_cb125r_scraper
from .scraper_pcx125 import run_pcx125_scraper
from .scraper_agility125 import run_agility125_scraper
from .scraper_z900 import run_z900_scraper
from .scraper_mt07 import run_mt07_scraper

# Mapeo de modelos a funciones de ejecución
SCRAPER_FUNCTIONS = {
    'cb125r': run_cb125r_scraper,
    'pcx125': run_pcx125_scraper,
    'agility125': run_agility125_scraper,
    'z900': run_z900_scraper,
    'mt07': run_mt07_scraper
}

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def get_scraper_class(modelo_key: str):
    """
    Obtener clase de scraper para un modelo específico
    
    Args:
        modelo_key: Clave del modelo (ej: 'cb125r')
        
    Returns:
        Clase del scraper correspondiente
        
    Raises:
        ValueError: Si el modelo no existe
    """
    if modelo_key not in SCRAPER_CLASSES:
        available_models = list(SCRAPER_CLASSES.keys())
        raise ValueError(f"Modelo '{modelo_key}' no disponible. Modelos disponibles: {available_models}")
    
    return SCRAPER_CLASSES[modelo_key]

def get_scraper_function(modelo_key: str):
    """
    Obtener función de ejecución para un modelo específico
    
    Args:
        modelo_key: Clave del modelo (ej: 'cb125r')
        
    Returns:
        Función de ejecución correspondiente
        
    Raises:
        ValueError: Si el modelo no existe
    """
    if modelo_key not in SCRAPER_FUNCTIONS:
        available_models = list(SCRAPER_FUNCTIONS.keys())
        raise ValueError(f"Modelo '{modelo_key}' no disponible. Modelos disponibles: {available_models}")
    
    return SCRAPER_FUNCTIONS[modelo_key]

def get_available_models():
    """
    Obtener lista de modelos disponibles
    
    Returns:
        Lista de claves de modelos disponibles
    """
    return list(SCRAPER_CLASSES.keys())

def create_scraper(modelo_key: str):
    """
    Crear instancia de scraper para un modelo específico
    
    Args:
        modelo_key: Clave del modelo (ej: 'cb125r')
        
    Returns:
        Instancia del scraper correspondiente
        
    Raises:
        ValueError: Si el modelo no existe
    """
    scraper_class = get_scraper_class(modelo_key)
    return scraper_class()

def run_scraper(modelo_key: str):
    """
    Ejecutar scraper para un modelo específico
    
    Args:
        modelo_key: Clave del modelo (ej: 'cb125r')
        
    Returns:
        DataFrame con resultados del scraping
        
    Raises:
        ValueError: Si el modelo no existe
    """
    scraper_function = get_scraper_function(modelo_key)
    return scraper_function()

# ============================================================================
# INFORMACIÓN DEL MÓDULO
# ============================================================================

__all__ = [
    # Clase base
    'BaseScraper',
    
    # Clases específicas
    'ScraperCB125R',
    'ScraperPCX125',
    'ScraperAGILITY125',
    'ScraperZ900',
    'ScraperMT07',
    
    # Funciones de ejecución
    'run_cb125r_scraper',
    'run_pcx125_scraper',
    'run_agility125_scraper',
    'run_z900_scraper',
    'run_mt07_scraper',

    
    # Mapeos
    'SCRAPER_CLASSES',
    'SCRAPER_FUNCTIONS',
    
    # Funciones de utilidad
    'get_scraper_class',
    'get_scraper_function',
    'get_available_models',
    'create_scraper',
    'run_scraper'
]

# Metadatos del módulo
__version__ = '1.0.0'
__author__ = 'Carlos Peraza'
__description__ = 'Scrapers específicos para modelos de motos de Wallapop'

# ============================================================================
# VALIDACIÓN DE IMPORTACIONES
# ============================================================================

def validate_scrapers():
    """Validar que todos los scrapers se pueden importar correctamente"""
    print(" Validando scrapers...")
    
    for modelo, scraper_class in SCRAPER_CLASSES.items():
        try:
            # Intentar crear instancia
            scraper = scraper_class()
            print(f" {modelo}: {scraper_class.__name__}")
        except Exception as e:
            print(f" {modelo}: Error al crear {scraper_class.__name__} - {e}")
    
    print(f"\n Total scrapers disponibles: {len(SCRAPER_CLASSES)}")

if __name__ == "__main__":
    # Ejecutar validación si se importa directamente
    validate_scrapers()