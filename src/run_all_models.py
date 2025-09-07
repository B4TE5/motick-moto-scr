"""
================================================================================
                    RUN ALL MODELS - WALLAPOP MOTOS SCRAPER                    
================================================================================

Script para ejecutar todos los modelos de motos de forma secuencial
Útil para testing local y verificación completa del sistema

ADVERTENCIA: Este script puede tardar varias horas en completarse
Se recomienda usar solo para testing o ejecuciones manuales específicas

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, List

# Configurar path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_all_modelos, get_modelo_config
from main_runner import MainRunner
from google_sheets_manager import GoogleSheetsManager

class AllModelsRunner:
    """Ejecutor de todos los modelos de motos"""
    
    def __init__(self, test_mode: bool = False, selected_models: List[str] = None):
        """
        Inicializar runner para todos los modelos
        
        Args:
            test_mode: Si ejecutar en modo prueba
            selected_models: Lista de modelos específicos a ejecutar (None = todos)
        """
        self.test_mode = test_mode
        self.selected_models = selected_models or get_all_modelos()
        
        # Configurar logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Estadísticas de ejecución
        self.stats = {
            'total_models': len(self.selected_models),
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None,
            'model_results': {}
        }
        
        self.logger.info(f" AllModelsRunner iniciado para {len(self.selected_models)} modelos")
        if test_mode:
            self.logger.info(" Modo de prueba activado")
    
    def _setup_logging(self):
        """Configurar logging específico para ejecución completa"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/all_models_{timestamp}.log"
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def run_all(self) -> Dict:
        """
        Ejecutar todos los modelos seleccionados
        
        Returns:
            Diccionario con estadísticas de ejecución
        """
        self.stats['start_time'] = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info(f" INICIANDO EJECUCIÓN DE {len(self.selected_models)} MODELOS")
        self.logger.info("=" * 80)
        
        # Verificar conexión inicial
        if not self._test_initial_connection():
            self.logger.error(" Error en conexión inicial - abortando ejecución")
            return self._finalize_stats()
        
        # Ejecutar cada modelo
        for i, modelo in enumerate(self.selected_models, 1):
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f" MODELO {i}/{len(self.selected_models)}: {modelo.upper()}")
            self.logger.info(f"{'='*60}")
            
            try:
                success = self._run_single_model(modelo, i)
                
                if success:
                    self.stats['successful'] += 1
                    self.stats['model_results'][modelo] = 'SUCCESS'
                    self.logger.info(f" {modelo} completado exitosamente")
                else:
                    self.stats['failed'] += 1
                    self.stats['model_results'][modelo] = 'FAILED'
                    self.logger.error(f" {modelo} falló")
                
                # Pausa entre modelos para evitar sobrecarga
                if i < len(self.selected_models):
                    self._inter_model_pause()
                    
            except KeyboardInterrupt:
                self.logger.warning(f" Ejecución interrumpida por usuario en modelo {modelo}")
                self.stats['model_results'][modelo] = 'INTERRUPTED'
                break
            except Exception as e:
                self.logger.error(f" Error crítico en modelo {modelo}: {e}")
                self.stats['failed'] += 1
                self.stats['model_results'][modelo] = f'ERROR: {str(e)}'
                continue
        
        return self._finalize_stats()
    
    def _test_initial_connection(self) -> bool:
        """Probar conexión inicial a Google Sheets"""
        try:
            self.logger.info(" Probando conexión inicial a Google Sheets...")
            sheets_manager = GoogleSheetsManager()
            success = sheets_manager.test_connection()
            
            if success:
                self.logger.info(" Conexión a Google Sheets verificada")
                return True
            else:
                self.logger.error(" Error en conexión a Google Sheets")
                return False
                
        except Exception as e:
            self.logger.error(f" Error probando conexión: {e}")
            return False
    
    def _run_single_model(self, modelo: str, model_number: int) -> bool:
        """Ejecutar un modelo específico"""
        try:
            modelo_config = get_modelo_config(modelo)
            self.logger.info(f" Configuración: {modelo_config['nombre']} ({modelo_config['tipo']})")
            
            # Crear y ejecutar runner
            runner = MainRunner(modelo, self.test_mode)
            success = runner.run()
            
            if success:
                self.logger.info(f" Modelo {modelo} ejecutado exitosamente")
            else:
                self.logger.error(f" Modelo {modelo} falló durante ejecución")
            
            return success
            
        except Exception as e:
            self.logger.error(f" Error ejecutando modelo {modelo}: {e}")
            return False
    
    def _inter_model_pause(self):
        """Pausa entre modelos para evitar sobrecarga"""
        pause_seconds = 30 if not self.test_mode else 10
        self.logger.info(f"⏸ Pausa de {pause_seconds} segundos antes del siguiente modelo...")
        time.sleep(pause_seconds)
    
    def _finalize_stats(self) -> Dict:
        """Finalizar estadísticas y mostrar resumen"""
        self.stats['end_time'] = datetime.now()
        
        if self.stats['start_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
            self.stats['duration'] = str(duration)
            self.stats['duration_minutes'] = duration.total_seconds() / 60
        
        self._log_final_summary()
        return self.stats
    
    def _log_final_summary(self):
        """Mostrar resumen final de la ejecución"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info(" RESUMEN FINAL DE EJECUCIÓN")
        self.logger.info("=" * 80)
        
        self.logger.info(f" Tiempo total: {self.stats.get('duration', 'N/A')}")
        self.logger.info(f" Modelos totales: {self.stats['total_models']}")
        self.logger.info(f" Exitosos: {self.stats['successful']}")
        self.logger.info(f" Fallidos: {self.stats['failed']}")
        
        if self.stats['total_models'] > 0:
            success_rate = (self.stats['successful'] / self.stats['total_models']) * 100
            self.logger.info(f" Tasa de éxito: {success_rate:.1f}%")
        
        self.logger.info("\n DETALLE POR MODELO:")
        for modelo, resultado in self.stats['model_results'].items():
            config = get_modelo_config(modelo)
            self.logger.info(f" {config['nombre']}: {resultado}")
        
        if self.stats['successful'] > 0:
            self.logger.info(f"\n Ver resultados en Google Sheets")
        
        self.logger.info("=" * 80)

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def run_selected_models(models: List[str], test_mode: bool = False) -> Dict:
    """
    Ejecutar modelos específicos
    
    Args:
        models: Lista de claves de modelos
        test_mode: Modo de prueba
        
    Returns:
        Diccionario con estadísticas
    """
    runner = AllModelsRunner(test_mode, models)
    return runner.run_all()

def run_by_category(category: str, test_mode: bool = False) -> Dict:
    """
    Ejecutar modelos por categoría
    
    Args:
        category: "125cc", "naked", "scooter", etc.
        test_mode: Modo de prueba
        
    Returns:
        Diccionario con estadísticas
    """
    category_models = {
        "125cc": ["cb125r", "pcx125", "agility125"],
        "naked": ["z900", "mt07"],
        "scooter": ["pcx125", "agility125"],
        "honda": ["cb125r", "pcx125"],
        "kawasaki": ["z900"],
        "yamaha": ["mt07"],
        "kymco": ["agility125"]
    }
    
    if category not in category_models:
        print(f" Categoría '{category}' no válida")
        print(f" Categorías disponibles: {list(category_models.keys())}")
        return {}
    
    models = category_models[category]
    print(f" Ejecutando categoría '{category}': {models}")
    
    return run_selected_models(models, test_mode)

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal con opciones de línea de comandos"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ejecutar múltiples modelos de motos")
    parser.add_argument("--test", action="store_true", help="Ejecutar en modo prueba")
    parser.add_argument("--models", nargs="+", help="Modelos específicos a ejecutar")
    parser.add_argument("--category", help="Ejecutar por categoría (125cc, naked, scooter, etc.)")
    parser.add_argument("--list", action="store_true", help="Listar modelos disponibles")
    
    args = parser.parse_args()
    
    if args.list:
        print(" Modelos disponibles:")
        for modelo in get_all_modelos():
            config = get_modelo_config(modelo)
            print(f"   • {modelo}: {config['nombre']} ({config['tipo']})")
        return
    
    if args.category:
        stats = run_by_category(args.category, args.test)
    elif args.models:
        # Validar modelos
        valid_models = get_all_modelos()
        invalid_models = [m for m in args.models if m not in valid_models]
        
        if invalid_models:
            print(f" Modelos no válidos: {invalid_models}")
            print(f" Modelos disponibles: {valid_models}")
            return
        
        stats = run_selected_models(args.models, args.test)
    else:
        # Ejecutar todos los modelos
        print(" Ejecutando TODOS los modelos")
        print(" ADVERTENCIA: Esto puede tardar varias horas")
        
        confirm = input("¿Continuar? (y/N): ")
        if confirm.lower() != 'y':
            print(" Ejecución cancelada")
            return
        
        runner = AllModelsRunner(args.test)
        stats = runner.run_all()
    
    # Mostrar resumen final
    if stats:
        print(f"\n Ejecución finalizada:")
        print(f"    Exitosos: {stats['successful']}")
        print(f"    Fallidos: {stats['failed']}")
        print(f"    Duración: {stats.get('duration', 'N/A')}")

if __name__ == "__main__":
    main()