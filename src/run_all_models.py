"""
================================================================================
                                RUN ALL MODELS                  
================================================================================

ADVERTENCIA: Con los timeouts extendidos, ejecutar todos los modelos puede tardar 
20-25 horas. Se recomienda ejecutar subconjuntos o modelos individuales.

Autor: Carlos Peraza
Versión: 1.5
Fecha: Agosto 2025

================================================================================
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List

# Configurar path para imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_all_modelos, get_modelo_config
from main_runner import MainRunner
from google_sheets_manager import GoogleSheetsManager

class AllModelsRunnerExtendido:
    """Ejecutor de múltiples modelos con manejo de timeouts extendidos"""
    
    def __init__(self, test_mode: bool = False, selected_models: List[str] = None, max_parallel: int = 1):
        """
        Inicializar runner para múltiples modelos con timeouts extendidos
        
        Args:
            test_mode: Si ejecutar en modo prueba
            selected_models: Lista de modelos específicos a ejecutar (None = todos)
            max_parallel: Número máximo de modelos en paralelo (recomendado: 1)
        """
        self.test_mode = test_mode
        self.selected_models = selected_models or get_all_modelos()
        self.max_parallel = max_parallel
        
        # Configurar logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Estadísticas de ejecución extendida
        self.stats = {
            'total_models': len(self.selected_models),
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None,
            'estimated_duration_hours': len(self.selected_models) * 4.5,  # 4.5h por modelo
            'model_results': {},
            'model_durations': {}
        }
        
        self.logger.info(f"AllModelsRunner EXTENDIDO iniciado para {len(self.selected_models)} modelos")
        self.logger.info(f"Duración estimada: {self.stats['estimated_duration_hours']:.1f} horas")
        if test_mode:
            self.logger.info("Modo de prueba activado")
    
    def _setup_logging(self):
        """Configurar logging específico para ejecución extendida"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/all_models_extendido_{timestamp}.log"
        
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
        Ejecutar todos los modelos seleccionados con timeouts extendidos
        
        Returns:
            Diccionario con estadísticas de ejecución
        """
        self.stats['start_time'] = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info(f"INICIANDO EJECUCIÓN EXTENDIDA DE {len(self.selected_models)} MODELOS")
        self.logger.info(f"DURACIÓN ESTIMADA: {self.stats['estimated_duration_hours']:.1f} HORAS")
        self.logger.info("=" * 80)
        
        # Verificar conexión inicial
        if not self._test_initial_connection():
            self.logger.error("Error en conexión inicial - abortando ejecución")
            return self._finalize_stats()
        
        # Mostrar advertencia para ejecuciones largas
        if len(self.selected_models) > 2 and not self.test_mode:
            self._show_duration_warning()
        
        # Ejecutar cada modelo con timeouts extendidos
        for i, modelo in enumerate(self.selected_models, 1):
            model_start_time = datetime.now()
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"MODELO {i}/{len(self.selected_models)}: {modelo.upper()}")
            self.logger.info(f"INICIO: {model_start_time.strftime('%H:%M:%S')}")
            self.logger.info(f"ESTIMADO FINALIZACIÓN: {(model_start_time + timedelta(hours=4.5)).strftime('%H:%M:%S')}")
            self.logger.info(f"{'='*60}")
            
            try:
                success = self._run_single_model_extended(modelo, i)
                
                model_end_time = datetime.now()
                model_duration = model_end_time - model_start_time
                self.stats['model_durations'][modelo] = model_duration
                
                if success:
                    self.stats['successful'] += 1
                    self.stats['model_results'][modelo] = 'SUCCESS'
                    self.logger.info(f"{modelo} completado exitosamente en {model_duration}")
                else:
                    self.stats['failed'] += 1
                    self.stats['model_results'][modelo] = 'FAILED'
                    self.logger.error(f"{modelo} falló después de {model_duration}")
                
                # Pausa entre modelos para evitar sobrecarga
                if i < len(self.selected_models):
                    self._inter_model_pause_extended()
                    
            except KeyboardInterrupt:
                self.logger.warning(f"Ejecución interrumpida por usuario en modelo {modelo}")
                self.stats['model_results'][modelo] = 'INTERRUPTED'
                break
            except Exception as e:
                self.logger.error(f"Error crítico en modelo {modelo}: {e}")
                self.stats['failed'] += 1
                self.stats['model_results'][modelo] = f'ERROR: {str(e)}'
                continue
        
        return self._finalize_stats()
    
    def run_subset(self, subset_name: str) -> Dict:
        """
        Ejecutar un subconjunto predefinido de modelos
        
        Args:
            subset_name: Nombre del subconjunto ('popular', 'honda', 'grandes', etc.)
            
        Returns:
            Diccionario con estadísticas
        """
        subsets = {
            'popular': ['cb125r', 'pcx125'],  # Modelos más populares (9h)
            'honda': ['cb125r', 'pcx125'],    # Solo Honda (9h)
            '125cc': ['cb125r', 'pcx125', 'agility125'],  # Motos 125cc (13.5h)
            'grandes': ['mt07', 'z900'],      # Motos grandes (9h)
            'scooters': ['pcx125', 'agility125'],  # Solo scooters (9h)
            'rapido': ['cb125r'],             # Solo un modelo (4.5h)
        }
        
        if subset_name not in subsets:
            self.logger.error(f"Subconjunto '{subset_name}' no válido")
            self.logger.info(f"Subconjuntos disponibles: {list(subsets.keys())}")
            return {}
        
        selected_models = subsets[subset_name]
        self.logger.info(f"Ejecutando subconjunto '{subset_name}': {selected_models}")
        
        # Crear nueva instancia con el subconjunto
        runner = AllModelsRunnerExtendido(self.test_mode, selected_models)
        return runner.run_all()
    
    def _show_duration_warning(self):
        """Mostrar advertencia sobre duración extendida"""
        estimated_end = self.stats['start_time'] + timedelta(hours=self.stats['estimated_duration_hours'])
        
        self.logger.warning("  ADVERTENCIA: EJECUCIÓN EXTENDIDA")
        self.logger.warning(f"  Duración estimada: {self.stats['estimated_duration_hours']:.1f} horas")
        self.logger.warning(f" Hora estimada de finalización: {estimated_end.strftime('%d/%m/%Y %H:%M:%S')}")
        self.logger.warning(" Considera usar subconjuntos para ejecuciones más cortas")
        self.logger.warning("=" * 80)
    
    def _test_initial_connection(self) -> bool:
        """Probar conexión inicial a Google Sheets"""
        try:
            self.logger.info("Probando conexión inicial a Google Sheets...")
            sheets_manager = GoogleSheetsManager()
            success = sheets_manager.test_connection()
            
            if success:
                self.logger.info("Conexión a Google Sheets verificada")
                return True
            else:
                self.logger.error("Error en conexión a Google Sheets")
                return False
                
        except Exception as e:
            self.logger.error(f"Error probando conexión: {e}")
            return False
    
    def _run_single_model_extended(self, modelo: str, model_number: int) -> bool:
        """Ejecutar un modelo específico con timeout extendido"""
        try:
            modelo_config = get_modelo_config(modelo)
            self.logger.info(f"Configuración: {modelo_config['nombre']} ({modelo_config['tipo']})")
            
            # Crear y ejecutar runner con timeout extendido
            runner = MainRunner(modelo, self.test_mode)
            
            # Establecer timeout extendido (4.5 horas = 16200 segundos)
            timeout_seconds = 16200 if not self.test_mode else 600  # 10 min en test
            
            start_time = time.time()
            success = runner.run()
            execution_time = time.time() - start_time
            
            self.logger.info(f"Tiempo de ejecución real: {execution_time/60:.1f} minutos")
            
            if success:
                self.logger.info(f"Modelo {modelo} ejecutado exitosamente")
            else:
                self.logger.error(f"Modelo {modelo} falló durante ejecución")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error ejecutando modelo {modelo}: {e}")
            return False
    
    def _inter_model_pause_extended(self):
        """Pausa extendida entre modelos para recuperación del sistema"""
        pause_minutes = 5 if not self.test_mode else 1
        self.logger.info(f"  Pausa de recuperación: {pause_minutes} minutos antes del siguiente modelo...")
        
        # Mostrar progreso durante la pausa
        for remaining in range(pause_minutes * 60, 0, -30):
            if remaining > 60:
                self.logger.debug(f"Pausa: {remaining//60}m {remaining%60}s restantes")
            time.sleep(30)
        
        self.logger.info("  Reanudando ejecución...")
    
    def _finalize_stats(self) -> Dict:
        """Finalizar estadísticas y mostrar resumen extendido"""
        self.stats['end_time'] = datetime.now()
        
        if self.stats['start_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
            self.stats['duration'] = str(duration)
            self.stats['duration_hours'] = duration.total_seconds() / 3600
        
        self._log_final_summary_extended()
        return self.stats
    
    def _log_final_summary_extended(self):
        """Mostrar resumen final extendido de la ejecución"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("RESUMEN FINAL DE EJECUCIÓN EXTENDIDA")
        self.logger.info("=" * 80)
        
        self.logger.info(f"  Tiempo total: {self.stats.get('duration', 'N/A')}")
        self.logger.info(f" Modelos totales: {self.stats['total_models']}")
        self.logger.info(f" Exitosos: {self.stats['successful']}")
        self.logger.info(f" Fallidos: {self.stats['failed']}")
        
        if self.stats['total_models'] > 0:
            success_rate = (self.stats['successful'] / self.stats['total_models']) * 100
            self.logger.info(f" Tasa de éxito: {success_rate:.1f}%")
        
        if 'duration_hours' in self.stats:
            estimated_hours = self.stats['estimated_duration_hours']
            actual_hours = self.stats['duration_hours']
            efficiency = (estimated_hours / actual_hours) * 100 if actual_hours > 0 else 0
            self.logger.info(f" Eficiencia temporal: {efficiency:.1f}% (estimado vs real)")
        
        self.logger.info("\n DETALLE POR MODELO:")
        for modelo, resultado in self.stats['model_results'].items():
            config = get_modelo_config(modelo)
            duration = self.stats['model_durations'].get(modelo, 'N/A')
            self.logger.info(f"    {config['nombre']}: {resultado} ({duration})")
        
        if self.stats['successful'] > 0:
            self.logger.info(f"\n🔗 Ver resultados en Google Sheets")
        
        self.logger.info("=" * 80)

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def run_selected_models_extended(models: List[str], test_mode: bool = False) -> Dict:
    """
    Ejecutar modelos específicos con timeouts extendidos
    
    Args:
        models: Lista de claves de modelos
        test_mode: Modo de prueba
        
    Returns:
        Diccionario con estadísticas
    """
    runner = AllModelsRunnerExtendido(test_mode, models)
    return runner.run_all()

def run_by_subset(subset_name: str, test_mode: bool = False) -> Dict:
    """
    Ejecutar modelos por subconjunto predefinido
    
    Args:
        subset_name: "popular", "honda", "125cc", "grandes", "scooters", "rapido"
        test_mode: Modo de prueba
        
    Returns:
        Diccionario con estadísticas
    """
    runner = AllModelsRunnerExtendido(test_mode)
    return runner.run_subset(subset_name)

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal con opciones extendidas de línea de comandos"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ejecutar múltiples modelos con timeouts extendidos")
    parser.add_argument("--test", action="store_true", help="Ejecutar en modo prueba")
    parser.add_argument("--models", nargs="+", help="Modelos específicos a ejecutar")
    parser.add_argument("--subset", help="Ejecutar subconjunto (popular, honda, 125cc, grandes, scooters, rapido)")
    parser.add_argument("--list", action="store_true", help="Listar opciones disponibles")
    parser.add_argument("--estimate", action="store_true", help="Mostrar estimaciones de tiempo")
    
    args = parser.parse_args()
    
    if args.list:
        print(" OPCIONES DISPONIBLES:")
        print("\n🏍️  MODELOS:")
        for modelo in get_all_modelos():
            config = get_modelo_config(modelo)
            print(f"   • {modelo}: {config['nombre']} ({config['tipo']})")
        
        print("\n SUBCONJUNTOS:")
        subsets = {
            'popular': 'CB125R + PCX125 (9h)',
            'honda': 'Solo Honda (9h)', 
            '125cc': 'Motos 125cc (13.5h)',
            'grandes': 'MT-07 + Z900 (9h)',
            'scooters': 'Solo scooters (9h)',
            'rapido': 'Solo CB125R (4.5h)'
        }
        for subset, desc in subsets.items():
            print(f"   • {subset}: {desc}")
        return
    
    if args.estimate:
        print("  ESTIMACIONES DE TIEMPO:")
        print("   • Por modelo: 4-5 horas")
        print("   • Todos los modelos (5): 20-25 horas")
        print("   • Subconjunto popular (2): 9-10 horas")
        print("   • Subconjunto rápido (1): 4-5 horas")
        print("\n Se recomienda usar subconjuntos para ejecuciones más manejables")
        return
    
    if args.subset:
        print(f" Ejecutando subconjunto '{args.subset}'")
        stats = run_by_subset(args.subset, args.test)
    elif args.models:
        # Validar modelos
        valid_models = get_all_modelos()
        invalid_models = [m for m in args.models if m not in valid_models]
        
        if invalid_models:
            print(f" Modelos no válidos: {invalid_models}")
            print(f" Modelos disponibles: {valid_models}")
            return
        
        estimated_hours = len(args.models) * 4.5
        print(f" Ejecutando {len(args.models)} modelos")
        print(f"  Duración estimada: {estimated_hours:.1f} horas")
        
        confirm = input("¿Continuar? (y/N): ")
        if confirm.lower() != 'y':
            print(" Ejecución cancelada")
            return
        
        stats = run_selected_models_extended(args.models, args.test)
    else:
        # Ejecutar todos los modelos
        print(" Ejecutando TODOS los modelos (5)")
        print("  ADVERTENCIA: Esto tardará 20-25 horas")
        
        confirm = input("¿Estás SEGURO de continuar? (y/N): ")
        if confirm.lower() != 'y':
            print(" Ejecución cancelada")
            return
        
        runner = AllModelsRunnerExtendido(args.test)
        stats = runner.run_all()
    
    # Mostrar resumen final
    if stats:
        print(f"\n EJECUCIÓN FINALIZADA:")
        print(f"    Exitosos: {stats['successful']}")
        print(f"    Fallidos: {stats['failed']}")
        print(f"    Duración: {stats.get('duration', 'N/A')}")

if __name__ == "__main__":
    main()
