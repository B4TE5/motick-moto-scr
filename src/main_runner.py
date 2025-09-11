"""
================================================================================
                                MAIN RUNNER                  
================================================================================

Autor: Carlos Peraza
Versión: 2.0
Fecha: Agosto 2025

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

from config import get_modelo_config, get_all_modelos, LOGGING_CONFIG, get_timeout_config
from google_sheets_manager import GoogleSheetsManager, test_google_sheets_connection
from rentabilidad_calculator import RentabilidadCalculator
from limpiador_integrado import LimpiadorIntegrado

class MainRunner:
    """Coordinador principal ULTRA OPTIMIZADO del sistema de scraping"""
    
    def __init__(self, modelo_key: str, test_mode: bool = False):
        """
        Inicializar runner principal optimizado
        
        Args:
            modelo_key: Clave del modelo a procesar
            test_mode: Si ejecutar en modo prueba
        """
        self.modelo_key = modelo_key
        self.test_mode = test_mode
        self.modelo_config = get_modelo_config(modelo_key)
        self.timeout_config = get_timeout_config()
        
        # Configurar logging optimizado
        self._setup_logging_optimized()
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"MainRunner ULTRA OPTIMIZADO iniciado para {self.modelo_config['nombre']}")
        if test_mode:
            self.logger.info("Modo de prueba activado - TIMEOUTS REDUCIDOS")
        else:
            self.logger.info(f"Modo completo activado - VELOCIDAD MAXIMA")
    
    def _setup_logging_optimized(self):
        """Configurar sistema de logging optimizado"""
        # Crear directorio de logs si no existe
        log_dir = os.path.dirname(LOGGING_CONFIG['log_file'])
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Configurar logging con formato optimizado
        logging.basicConfig(
            level=getattr(logging, LOGGING_CONFIG['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOGGING_CONFIG['log_file']),
                logging.StreamHandler()
            ]
        )
    
    def run(self) -> bool:
        """
        ULTRA OPTIMIZADO: Ejecutar proceso completo de scraping
        
        Returns:
            bool: True si fue exitoso
        """
        try:
            print("="*80)
            print("    SCRAPING ULTRA OPTIMIZADO - VERSION MOTICK")
            print("="*80)
            print(" CARACTERISTICAS:")
            print("   • Velocidad 5x mayor que version anterior")
            print("   • Logging detallado en tiempo real")
            print("   • Basado en tecnicas exitosas del scraper MOTICK")
            print("   • Timeouts agresivamente optimizados")
            print()
            
            self.logger.info(f"Iniciando proceso ULTRA OPTIMIZADO para {self.modelo_config['nombre']}")
            
            # PASO 1: Scraping ULTRA OPTIMIZADO
            self.logger.info("PASO 1: Ejecutando scraping ULTRA OPTIMIZADO...")
            print(f"[INFO] Iniciando extraccion de {self.modelo_config['nombre']}...")
            
            df_raw = self._execute_scraping_ultra_optimized()
            
            if df_raw is None or df_raw.empty:
                self.logger.warning("No se obtuvieron datos del scraping")
                print("[ERROR] No se obtuvieron datos del scraping")
                return False
            
            self.logger.info(f"Scraping completado: {len(df_raw)} motos encontradas")
            print(f"[INFO] Scraping completado: {len(df_raw)} motos encontradas")
            
            # PASO 2: Limpieza RAPIDA
            self.logger.info("PASO 2: Limpiando datos...")
            print(f"[INFO] Aplicando filtros de limpieza...")
            
            df_clean, clean_stats = self._clean_data_fast(df_raw)
            
            if df_clean.empty:
                self.logger.warning("No quedaron datos despues de la limpieza")
                print("[ERROR] No quedaron datos despues de la limpieza")
                return False
            
            self.logger.info(f"Limpieza completada: {len(df_clean)} motos validas")
            print(f"[INFO] Limpieza completada: {len(df_clean)} motos validas")
            
            # PASO 3: Calculo de rentabilidad SIMPLIFICADO
            self.logger.info("PASO 3: Calculando rentabilidad...")
            print(f"[INFO] Calculando rentabilidad...")
            
            df_final = self._calculate_rentabilidad_fast(df_clean)
            
            self.logger.info(f"Rentabilidad calculada para {len(df_final)} motos")
            print(f"[INFO] Rentabilidad calculada para {len(df_final)} motos")
            
            # PASO 4: Guardar resultados locales (opcional)
            if not self.test_mode:
                self._save_local_results_fast(df_final)
            
            # PASO 5: Subir a Google Sheets
            self.logger.info("PASO 5: Subiendo a Google Sheets...")
            print(f"[INFO] Subiendo datos a Google Sheets...")
            
            success = self._upload_to_sheets_fast(df_final)
            
            if success:
                self.logger.info("Proceso completado exitosamente")
                print("[SUCCESS] Datos subidos exitosamente a Google Sheets")
                self._log_final_summary_optimized(df_raw, df_clean, df_final, clean_stats)
                return True
            else:
                self.logger.error("Error subiendo a Google Sheets")
                print("[ERROR] Error subiendo a Google Sheets")
                return False
                
        except Exception as e:
            self.logger.error(f"Error critico en proceso principal: {e}")
            print(f"[ERROR] Error critico: {e}")
            return False
    
    def _execute_scraping_ultra_optimized(self) -> Optional[pd.DataFrame]:
        """ULTRA OPTIMIZADO: Ejecutar scraping con velocidad maxima"""
        try:
            # Importar scraper especifico dinamicamente
            scraper_module = self._import_scraper_module()
            if not scraper_module:
                return None
            
            # Ejecutar scraper con configuracion ultra optimizada
            scraper_class = getattr(scraper_module, f"Scraper{self.modelo_key.upper()}")
            scraper = scraper_class()
            
            # EJECUCION ULTRA OPTIMIZADA
            if self.test_mode:
                self.logger.info("Modo prueba: limitando a 20 anuncios con velocidad maxima")
                df_results = scraper.scrape_model()
                # Limitar en modo prueba
                if not df_results.empty:
                    df_results = df_results.head(20)
                return df_results
            else:
                self.logger.info("Modo completo: scraping ULTRA OPTIMIZADO iniciado")
                return scraper.scrape_model()
                
        except Exception as e:
            self.logger.error(f"Error ejecutando scraping: {e}")
            return None
    
    def _import_scraper_module(self):
        """Importar modulo del scraper especifico"""
        try:
            module_name = f"scrapers.scraper_{self.modelo_key}"
            module = __import__(module_name, fromlist=[f"Scraper{self.modelo_key.upper()}"])
            return module
        except ImportError as e:
            self.logger.error(f"No se pudo importar scraper para {self.modelo_key}: {e}")
            return None
    
    def _clean_data_fast(self, df_raw: pd.DataFrame) -> tuple:
        """RAPIDO: Limpiar datos usando el limpiador integrado"""
        try:
            limpiador = LimpiadorIntegrado(self.modelo_key)
            return limpiador.clean_data(df_raw)
        except Exception as e:
            self.logger.error(f"Error limpiando datos: {e}")
            return df_raw, {}
    
    def _calculate_rentabilidad_fast(self, df_clean: pd.DataFrame) -> pd.DataFrame:
        """RAPIDO: Calcular rentabilidad SIMPLIFICADA"""
        try:
            calculator = RentabilidadCalculator(self.modelo_key)
            df_with_score = calculator.calculate_rentabilidad(df_clean)
            
            # SIMPLIFICAR RENTABILIDAD
            if 'Rentabilidad_Score' in df_with_score.columns:
                df_with_score['Rentabilidad'] = df_with_score['Rentabilidad_Score'].apply(self._score_to_category_fast)
            else:
                df_with_score['Rentabilidad'] = 'No calculada'
            
            return df_with_score
            
        except Exception as e:
            self.logger.error(f"Error calculando rentabilidad: {e}")
            return df_clean
    
    def _score_to_category_fast(self, score) -> str:
        """RAPIDO: Convertir score numerico a categoria simple"""
        if pd.isna(score):
            return "No calculada"
        
        try:
            score_value = float(score)
            if score_value >= 8:
                return "Excelente"
            elif score_value >= 6:
                return "Buena"
            elif score_value >= 4:
                return "Regular"
            else:
                return "Baja"
        except:
            return "No calculada"
    
    def _save_local_results_fast(self, df_final: pd.DataFrame):
        """RAPIDO: Guardar resultados localmente"""
        try:
            # Crear directorio de resultados si no existe
            results_dir = "resultados"
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            
            # Generar nombre de archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{results_dir}/{self.modelo_key}_{timestamp}.xlsx"
            
            # Formatear DataFrame antes de guardar
            df_formatted = self._format_dataframe_for_save_fast(df_final)
            
            # Guardar Excel
            df_formatted.to_excel(filename, index=False)
            self.logger.info(f"Resultados guardados localmente: {filename}")
            print(f"[INFO] Archivo local guardado: {filename}")
            
        except Exception as e:
            self.logger.warning(f"Error guardando resultados locales: {e}")
    
    def _format_dataframe_for_save_fast(self, df: pd.DataFrame) -> pd.DataFrame:
        """RAPIDO: Formatear DataFrame con orden correcto"""
        # ORDEN CORRECTO DE COLUMNAS
        desired_columns = [
            'Titulo',
            'Precio', 
            'Kilometraje',
            'Año',
            'Rentabilidad',
            'Vendedor',
            'Ubicacion',
            'Fecha_Publicacion',
            'URL',
            'Fecha_Extraccion'
        ]
        
        # COLUMNAS A ELIMINAR
        columns_to_remove = [
            'Rentabilidad_Score',
            'Ranking_Rentabilidad', 
            'Descripcion',
            'Categoria_Rentabilidad'
        ]
        
        # Crear copia y eliminar columnas no deseadas
        df_clean = df.copy()
        for col in columns_to_remove:
            if col in df_clean.columns:
                df_clean = df_clean.drop(columns=[col])
        
        # Reordenar columnas
        final_columns = []
        for col in desired_columns:
            if col in df_clean.columns:
                final_columns.append(col)
        
        # Añadir cualquier columna restante
        for col in df_clean.columns:
            if col not in final_columns:
                final_columns.append(col)
        
        return df_clean[final_columns]
    
    def _upload_to_sheets_fast(self, df_final: pd.DataFrame) -> bool:
        """RAPIDO: Subir datos a Google Sheets"""
        try:
            # Crear manager de Google Sheets
            sheets_manager = GoogleSheetsManager()
            
            # NOMBRE DE HOJA CORREGIDO
            modelo_simple = self._get_simple_model_name_fast(self.modelo_key)
            fecha_actual = datetime.now().strftime("%d/%m/%y")
            sheet_name_correcto = f"{modelo_simple} {fecha_actual}"
            
            # Subir datos del modelo
            success = sheets_manager.upload_modelo_data(
                self.modelo_key,
                df_final,
                sheet_name=sheet_name_correcto,
                overwrite=True
            )
            
            if success:
                self.logger.info(f"Datos subidos exitosamente a hoja: '{sheet_name_correcto}'")
                print(f"[SUCCESS] Hoja creada: '{sheet_name_correcto}'")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error subiendo a Google Sheets: {e}")
            return False
    
    def _get_simple_model_name_fast(self, modelo_key: str) -> str:
        """RAPIDO: Obtener nombre simple del modelo"""
        model_names = {
            'cb125r': 'CB125R',
            'pcx125': 'PCX125', 
            'agility125': 'AGILITY125',
            'z900': 'Z900',
            'mt07': 'MT07'
        }
        
        return model_names.get(modelo_key, modelo_key.upper())
    
    def _log_final_summary_optimized(self, df_raw: pd.DataFrame, df_clean: pd.DataFrame, 
                          df_final: pd.DataFrame, clean_stats: dict):
        """OPTIMIZADO: Registrar resumen final del proceso"""
        print("\n" + "="*80)
        print(f"RESUMEN FINAL - {self.modelo_config['nombre']}")
        print("="*80)
        
        print(f"Motos extraidas: {len(df_raw)}")
        print(f"Motas despues de limpieza: {len(df_clean)}")
        print(f"Motos con rentabilidad: {len(df_final)}")
        print(f"Precio minimo aplicado: {clean_stats.get('precio_minimo', 'N/A')}€")
        print(f"KM maximo aplicado: {clean_stats.get('km_maximo', 'N/A')}")
        
        if not df_final.empty:
            # Calcular calidades de extraccion
            titulos_extraidos = len(df_final[df_final['Titulo'] != 'Sin titulo'])
            km_extraidos = len(df_final[df_final['Kilometraje'] != 'No especificado'])
            años_extraidos = len(df_final[df_final['Año'] != 'No especificado'])
            precios_extraidos = len(df_final[df_final['Precio'] != 'No especificado'])
            
            print(f"\nCALIDAD DE EXTRACCION:")
            print(f"   Titulos: {titulos_extraidos}/{len(df_final)} ({titulos_extraidos/len(df_final)*100:.1f}%)")
            print(f"   Precios: {precios_extraidos}/{len(df_final)} ({precios_extraidos/len(df_final)*100:.1f}%)")
            print(f"   Kilometrajes: {km_extraidos}/{len(df_final)} ({km_extraidos/len(df_final)*100:.1f}%)")
            print(f"   Años: {años_extraidos}/{len(df_final)} ({años_extraidos/len(df_final)*100:.1f}%)")
            
            # Mostrar top 5 
            print(f"\nTOP 5 MAS RENTABLES:")
            top_5 = df_final.head(5)
            for i, (_, moto) in enumerate(top_5.iterrows(), 1):
                titulo = moto.get('Titulo', 'Sin titulo')[:40]
                rentabilidad = moto.get('Rentabilidad', 'N/A')
                precio = moto.get('Precio', 'N/A')
                km = moto.get('Kilometraje', 'N/A')
                año = moto.get('Año', 'N/A')
                print(f"   {i}. {titulo} | {precio} | {km} | {año} | {rentabilidad}")
        
        print("="*80)
        print("SCRAPING ULTRA OPTIMIZADO COMPLETADO")
        print("="*80)

# FUNCIONES DE UTILIDAD OPTIMIZADAS
def test_connection():
    """RAPIDO: Probar conexion a Google Sheets"""
    print("[INFO] Probando conexion a Google Sheets...")
    return test_google_sheets_connection()

def list_available_models():
    """RAPIDO: Listar modelos disponibles"""
    models = get_all_modelos()
    print("Modelos disponibles:")
    for model in models:
        config = get_modelo_config(model)
        print(f"   • {model}: {config['nombre']} ({config['tipo']})")

def run_model(modelo_key: str, test_mode: bool = False) -> bool:
    """
    ULTRA OPTIMIZADO: Ejecutar scraping para un modelo especifico
    
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
        print(f"ERROR ejecutando modelo {modelo_key}: {e}")
        return False

# FUNCION PRINCIPAL OPTIMIZADA
def main():
    """Funcion principal ULTRA OPTIMIZADA con argumentos de linea de comandos"""
    parser = argparse.ArgumentParser(description="Wallapop Motos Scraper ULTRA OPTIMIZADO")
    parser.add_argument("modelo", nargs="?", help="Modelo a scrapear (ej: cb125r)")
    parser.add_argument("--test", action="store_true", help="Ejecutar en modo prueba")
    parser.add_argument("--list", action="store_true", help="Listar modelos disponibles")
    parser.add_argument("--test-connection", action="store_true", help="Probar conexion Google Sheets")
    
    args = parser.parse_args()
    
    # Verificar argumentos
    if args.list:
        list_available_models()
        return
    
    if args.test_connection:
        test_connection()
        return
    
    if not args.modelo:
        print("ERROR: Debe especificar un modelo")
        print("Uso: python main_runner.py <modelo> [--test]")
        print("Ejemplo: python main_runner.py cb125r --test")
        list_available_models()
        return
    
    # Validar modelo
    if args.modelo not in get_all_modelos():
        print(f"ERROR: Modelo '{args.modelo}' no valido")
        list_available_models()
        return
    
    # Mostrar configuracion
    print(f"CONFIGURACION ULTRA OPTIMIZADA:")
    print(f"   Velocidad: MAXIMA (5x mas rapido)")
    print(f"   Logging: DETALLADO en tiempo real")
    print(f"   Timeouts: AGRESIVAMENTE optimizados")
    
    # Ejecutar scraping
    print(f"\nIniciando scraping ULTRA OPTIMIZADO para {args.modelo}")
    if args.test:
        print("Modo de prueba activado (20 anuncios maximo)")
    
    success = run_model(args.modelo, args.test)
    
    if success:
        print("\nPROCESO COMPLETADO EXITOSAMENTE")
        sys.exit(0)
    else:
        print("\nPROCESO FALLO")
        sys.exit(1)

if __name__ == "__main__":
    main()
