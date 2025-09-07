"""
================================================================================
                    MAIN RUNNER - WALLAPOP MOTOS SCRAPER                    
================================================================================

Script principal que coordina todo el proceso de scraping:
1. Scraping del modelo específico
2. Limpieza automática de datos  
3. Cálculo de rentabilidad
4. Subida a Google Sheets

Se ejecuta desde GitHub Actions con el modelo como parámetro

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

import os
import sys
import logging
import argparse
import pandas as pd
from datetime import datetime
from typing import Optional

# Configurar path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_modelo_config, get_all_modelos, LOGGING_CONFIG
from google_sheets_manager import GoogleSheetsManager, test_google_sheets_connection
from rentabilidad_calculator import RentabilidadCalculator
from limpiador_integrado import LimpiadorIntegrado

class MainRunner:
    """Coordinador principal del sistema de scraping"""
    
    def __init__(self, modelo_key: str, test_mode: bool = False):
        """
        Inicializar runner principal
        
        Args:
            modelo_key: Clave del modelo a procesar
            test_mode: Si ejecutar en modo prueba
        """
        self.modelo_key = modelo_key
        self.test_mode = test_mode
        self.modelo_config = get_modelo_config(modelo_key)
        
        # Configurar logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f" MainRunner iniciado para {self.modelo_config['nombre']}")
        if test_mode:
            self.logger.info(" Modo de prueba activado")
    
    def _setup_logging(self):
        """Configurar sistema de logging"""
        # Crear directorio de logs si no existe
        log_dir = os.path.dirname(LOGGING_CONFIG['log_file'])
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Configurar logging
        logging.basicConfig(
            level=getattr(logging, LOGGING_CONFIG['level']),
            format=LOGGING_CONFIG['format'],
            handlers=[
                logging.FileHandler(LOGGING_CONFIG['log_file']),
                logging.StreamHandler()
            ]
        )
    
    def run(self) -> bool:
        """
        Ejecutar proceso completo de scraping para el modelo
        
        Returns:
            bool: True si fue exitoso
        """
        try:
            self.logger.info(f" Iniciando proceso completo para {self.modelo_config['nombre']}")
            
            # PASO 1: Scraping
            self.logger.info(" PASO 1: Ejecutando scraping...")
            df_raw = self._execute_scraping()
            
            if df_raw is None or df_raw.empty:
                self.logger.warning(" No se obtuvieron datos del scraping")
                return False
            
            self.logger.info(f" Scraping completado: {len(df_raw)} motos encontradas")
            
            # PASO 2: Limpieza
            self.logger.info(" PASO 2: Limpiando datos...")
            df_clean, clean_stats = self._clean_data(df_raw)
            
            if df_clean.empty:
                self.logger.warning(" No quedaron datos después de la limpieza")
                return False
            
            self.logger.info(f" Limpieza completada: {len(df_clean)} motos válidas")
            
            # PASO 3: Cálculo de rentabilidad
            self.logger.info(" PASO 3: Calculando rentabilidad...")
            df_final = self._calculate_rentabilidad(df_clean)
            
            self.logger.info(f" Rentabilidad calculada para {len(df_final)} motos")
            
            # PASO 4: Guardar resultados locales (opcional)
            if not self.test_mode:
                self._save_local_results(df_final)
            
            # PASO 5: Subir a Google Sheets
            self.logger.info(" PASO 5: Subiendo a Google Sheets...")
            success = self._upload_to_sheets(df_final)
            
            if success:
                self.logger.info(" Proceso completado exitosamente")
                self._log_final_summary(df_raw, df_clean, df_final, clean_stats)
                return True
            else:
                self.logger.error(" Error subiendo a Google Sheets")
                return False
                
        except Exception as e:
            self.logger.error(f" Error crítico en proceso principal: {e}")
            return False
    
    def _execute_scraping(self) -> Optional[pd.DataFrame]:
        """Ejecutar scraping según el modelo específico"""
        try:
            # Importar scraper específico dinámicamente
            scraper_module = self._import_scraper_module()
            if not scraper_module:
                return None
            
            # Ejecutar scraper
            scraper_class = getattr(scraper_module, f"Scraper{self.modelo_key.upper()}")
            scraper = scraper_class()
            
            # Ejecutar scraping
            if self.test_mode:
                self.logger.info(" Modo prueba: limitando resultados")
                df_results = scraper.scrape_model()
                # Limitar a 10 resultados en modo prueba
                if not df_results.empty:
                    df_results = df_results.head(10)
                return df_results
            else:
                return scraper.scrape_model()
                
        except Exception as e:
            self.logger.error(f" Error ejecutando scraping: {e}")
            return None
    
    def _import_scraper_module(self):
        """Importar módulo del scraper específico"""
        try:
            module_name = f"scrapers.scraper_{self.modelo_key}"
            module = __import__(module_name, fromlist=[f"Scraper{self.modelo_key.upper()}"])
            return module
        except ImportError as e:
            self.logger.error(f" No se pudo importar scraper para {self.modelo_key}: {e}")
            
            # Lista de scrapers disponibles para debugging
            available_scrapers = self._get_available_scrapers()
            self.logger.info(f" Scrapers disponibles: {available_scrapers}")
            return None
    
    def _get_available_scrapers(self) -> list:
        """Obtener lista de scrapers disponibles"""
        scrapers_dir = os.path.join(os.path.dirname(__file__), "scrapers")
        available = []
        
        if os.path.exists(scrapers_dir):
            for file in os.listdir(scrapers_dir):
                if file.startswith("scraper_") and file.endswith(".py"):
                    model = file.replace("scraper_", "").replace(".py", "")
                    available.append(model)
        
        return available
    
    def _clean_data(self, df_raw: pd.DataFrame) -> tuple:
        """Limpiar datos usando el limpiador integrado"""
        try:
            limpiador = LimpiadorIntegrado(self.modelo_key)
            return limpiador.clean_data(df_raw)
        except Exception as e:
            self.logger.error(f" Error limpiando datos: {e}")
            return df_raw, {}
    
    def _calculate_rentabilidad(self, df_clean: pd.DataFrame) -> pd.DataFrame:
        """Calcular rentabilidad y ordenar"""
        try:
            calculator = RentabilidadCalculator(self.modelo_key)
            return calculator.calculate_rentabilidad(df_clean)
        except Exception as e:
            self.logger.error(f" Error calculando rentabilidad: {e}")
            return df_clean
    
    def _save_local_results(self, df_final: pd.DataFrame):
        """Guardar resultados localmente"""
        try:
            # Crear directorio de resultados si no existe
            results_dir = "resultados"
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            
            # Generar nombre de archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{results_dir}/{self.modelo_key}_{timestamp}.xlsx"
            
            # Guardar Excel
            df_final.to_excel(filename, index=False)
            self.logger.info(f" Resultados guardados localmente: {filename}")
            
        except Exception as e:
            self.logger.warning(f" Error guardando resultados locales: {e}")
    
    def _upload_to_sheets(self, df_final: pd.DataFrame) -> bool:
        """Subir datos a Google Sheets"""
        try:
            # Crear manager de Google Sheets
            sheets_manager = GoogleSheetsManager()
            
            # Subir datos del modelo
            sheet_name = self.modelo_config['sheet_name']
            success = sheets_manager.upload_modelo_data(
                self.modelo_key,
                df_final,
                sheet_name,
                overwrite=True
            )
            
            if success:
                # Actualizar hoja resumen
                sheets_manager.create_summary_sheet()
            
            return success
            
        except Exception as e:
            self.logger.error(f" Error subiendo a Google Sheets: {e}")
            return False
    
    def _log_final_summary(self, df_raw: pd.DataFrame, df_clean: pd.DataFrame, 
                          df_final: pd.DataFrame, clean_stats: dict):
        """Registrar resumen final del proceso"""
        self.logger.info("=" * 70)
        self.logger.info(f" RESUMEN FINAL - {self.modelo_config['nombre']}")
        self.logger.info("=" * 70)
        self.logger.info(f" Motos extraídas: {len(df_raw)}")
        self.logger.info(f" Motos después de limpieza: {len(df_clean)}")
        self.logger.info(f" Motos con rentabilidad: {len(df_final)}")
        self.logger.info(f" Precio mínimo aplicado: {clean_stats.get('precio_minimo', 'N/A')}€")
        self.logger.info(f" KM máximo aplicado: {clean_stats.get('km_maximo', 'N/A')}")
        
        if not df_final.empty and 'Rentabilidad_Score' in df_final.columns:
            top_3 = df_final.head(3)
            self.logger.info(f" TOP 3 MÁS RENTABLES:")
            for i, (_, moto) in enumerate(top_3.iterrows(), 1):
                titulo = moto.get('Título', 'Sin título')[:40]
                score = moto.get('Rentabilidad_Score', 0)
                precio = moto.get('Precio', 'N/A')
                self.logger.info(f"   {i}. {titulo} - Score: {score:.2f} - {precio}")
        
        self.logger.info("=" * 70)

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def test_connection():
    """Probar conexión a Google Sheets"""
    print(" Probando conexión a Google Sheets...")
    return test_google_sheets_connection()

def list_available_models():
    """Listar modelos disponibles"""
    models = get_all_modelos()
    print(" Modelos disponibles:")
    for model in models:
        config = get_modelo_config(model)
        print(f"   • {model}: {config['nombre']} ({config['tipo']})")

def run_model(modelo_key: str, test_mode: bool = False) -> bool:
    """
    Ejecutar scraping para un modelo específico
    
    Args:
        modelo_key: Clave del modelo
        test_mode: Modo de prueba
        
    Returns:
        bool: True si fue exitoso
    """
    try:
        runner = MainRunner(modelo_key, test_mode)
        return runner.run()
    except Exception as e:
        print(f" Error ejecutando modelo {modelo_key}: {e}")
        return False

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal con argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description="Wallapop Motos Scraper")
    parser.add_argument("modelo", nargs="?", help="Modelo a scrapear (ej: cb125r)")
    parser.add_argument("--test", action="store_true", help="Ejecutar en modo prueba")
    parser.add_argument("--list", action="store_true", help="Listar modelos disponibles")
    parser.add_argument("--test-connection", action="store_true", help="Probar conexión Google Sheets")
    
    args = parser.parse_args()
    
    # Verificar argumentos
    if args.list:
        list_available_models()
        return
    
    if args.test_connection:
        test_connection()
        return
    
    if not args.modelo:
        print(" Error: Debe especificar un modelo")
        print("Uso: python main_runner.py <modelo> [--test]")
        print("Ejemplo: python main_runner.py cb125r --test")
        list_available_models()
        return
    
    # Validar modelo
    if args.modelo not in get_all_modelos():
        print(f" Error: Modelo '{args.modelo}' no válido")
        list_available_models()
        return
    
    # Ejecutar scraping
    print(f" Iniciando scraping para {args.modelo}")
    if args.test:
        print(" Modo de prueba activado")
    
    success = run_model(args.modelo, args.test)
    
    if success:
        print(" Proceso completado exitosamente")
        sys.exit(0)
    else:
        print(" Proceso falló")
        sys.exit(1)

if __name__ == "__main__":
    main()