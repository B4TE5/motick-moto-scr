"""
================================================================================
                          SCRAPER CB125R FUNCIONAL                     
================================================================================

Autor: Carlos Peraza
Version: FUNCIONAL 
Fecha: Septiembre 2025

OBJETIVO: Conseguir datos reales de CB125R con la clase correcta para main_runner.py

================================================================================
"""

import re
import logging
from typing import Dict, List, Optional
from scrapers.base_scraper import BaseScraper

class ScraperCB125R(BaseScraper):
    """Scraper CB125R con la clase correcta para main_runner.py"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Debug stats
        self.validation_stats = {
            'total_processed': 0,
            'empty_title': 0,
            'no_honda': 0,
            'no_cb125r': 0,
            'excluded_model': 0,
            'invalid_price': 0,
            'successful': 0
        }
        
        self.logger.info(f"Scraper CB125R inicializado - Debug activado")
    
    def get_search_urls(self) -> List[str]:
        """URLs optimizadas para CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        # URLs básicas pero efectivas
        urls = [
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb%20125%20r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}&order_by=price_low_to_high",
            "https://es.wallapop.com/app/search?keywords=honda%20cb125r",
            "https://es.wallapop.com/app/search?keywords=cb125r",
            f"https://es.wallapop.com/app/search?keywords=moto%20honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%202020&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%202021&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%202022&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20deportiva%20125&min_sale_price={min_price}&max_sale_price={max_price}",
        ]
        
        self.logger.info(f"Generadas {len(urls)} URLs para CB125R")
        return urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validación mejorada con debug extenso"""
        self.validation_stats['total_processed'] += 1
        
        try:
            titulo = moto_data.get('Titulo', '').lower().strip()
            precio = moto_data.get('Precio', '')
            
            # DEBUG: Log datos para los primeros 5
            if self.validation_stats['total_processed'] <= 5:
                self.logger.info(f"[VALIDACION {self.validation_stats['total_processed']}]")
                self.logger.info(f"  Titulo: '{titulo}'")
                self.logger.info(f"  Precio: '{precio}'")
            
            # 1. Verificar título no vacío
            if not titulo or titulo == "sin titulo":
                self.validation_stats['empty_title'] += 1
                if self.validation_stats['total_processed'] <= 5:
                    self.logger.info(f"   Rechazado: título vacío")
                return False
            
            # 2. Verificar Honda (más flexible)
            honda_keywords = ['honda', 'hond']
            honda_found = any(keyword in titulo for keyword in honda_keywords)
            
            # 3. Verificar CB125R (muy flexible)
            cb125r_patterns = [
                'cb125r', 'cb 125 r', 'cb-125-r', 'cb 125r', 'cb125 r',
                'cb125', 'cb 125', 'honda 125'
            ]
            cb125r_found = any(pattern in titulo for pattern in cb125r_patterns)
            
            # 4. Excluir modelos claramente diferentes
            excluded_models = ['cb125f', 'cb250r', 'cb500r', 'cbr125', 'cbr']
            excluded = any(model in titulo for model in excluded_models)
            
            if excluded:
                self.validation_stats['excluded_model'] += 1
                if self.validation_stats['total_processed'] <= 5:
                    self.logger.info(f"   Rechazado: modelo excluido")
                return False
            
            # LÓGICA DE ACEPTACIÓN MÁS FLEXIBLE:
            
            # Caso 1: Honda + CB125R específico
            if honda_found and cb125r_found:
                self.validation_stats['successful'] += 1
                if self.validation_stats['total_processed'] <= 5:
                    self.logger.info(f"   Aceptado: Honda + CB125R")
                return True
            
            # Caso 2: Solo CB125R (sin Honda explícita pero patrón claro)
            if any(p in titulo for p in ['cb125r', 'cb 125 r']):
                self.validation_stats['successful'] += 1
                if self.validation_stats['total_processed'] <= 5:
                    self.logger.info(f"   Aceptado: CB125R específico")
                return True
            
            # Caso 3: Honda + precio en rango CB125R
            if honda_found and self._is_cb125r_price_range(precio):
                # Buscar indicios de 125cc
                if any(p in titulo for p in ['125', 'deportiva']):
                    self.validation_stats['successful'] += 1
                    if self.validation_stats['total_processed'] <= 5:
                        self.logger.info(f"   Aceptado: Honda + precio + 125")
                    return True
            
            # Si no cumple criterios
            if not honda_found:
                self.validation_stats['no_honda'] += 1
            elif not cb125r_found:
                self.validation_stats['no_cb125r'] += 1
            
            if self.validation_stats['total_processed'] <= 5:
                self.logger.info(f"   Rechazado: no cumple criterios")
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error en validación: {e}")
            return False
    
    def _is_cb125r_price_range(self, precio_text: str) -> bool:
        """Verificar si está en rango de precio CB125R"""
        if not precio_text or precio_text == "No especificado":
            return False
        
        try:
            numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', ''))
            if numbers:
                price = int(numbers[0])
                min_price = self.modelo_config['precio_min']
                max_price = self.modelo_config['precio_max']
                return min_price <= price <= max_price
        except:
            pass
        
        return False
    
    def scrape_model(self):
        """Override para mostrar stats al final"""
        try:
            result = super().scrape_model()
            self._show_validation_summary()
            return result
        except Exception as e:
            self.logger.error(f"Error en scrape_model: {e}")
            self._show_validation_summary()
            raise
    
    def _show_validation_summary(self):
        """Mostrar resumen de validación"""
        stats = self.validation_stats
        total = stats['total_processed']
        
        if total > 0:
            self.logger.info("="*60)
            self.logger.info("RESUMEN DE VALIDACIÓN CB125R")
            self.logger.info("="*60)
            self.logger.info(f"Total procesados: {total}")
            self.logger.info(f"Exitosos: {stats['successful']} ({stats['successful']/total*100:.1f}%)")
            self.logger.info("RECHAZOS:")
            self.logger.info(f"  Título vacío: {stats['empty_title']} ({stats['empty_title']/total*100:.1f}%)")
            self.logger.info(f"  Sin Honda: {stats['no_honda']} ({stats['no_honda']/total*100:.1f}%)")
            self.logger.info(f"  Sin CB125R: {stats['no_cb125r']} ({stats['no_cb125r']/total*100:.1f}%)")
            self.logger.info(f"  Modelo excluido: {stats['excluded_model']} ({stats['excluded_model']/total*100:.1f}%)")
            self.logger.info("="*60)
            
            # Diagnóstico
            if stats['successful'] == 0:
                self.logger.warning(" NINGUNA VALIDACIÓN EXITOSA")
                if stats['empty_title'] > total * 0.5:
                    self.logger.warning("   - PROBLEMA: Extractor de títulos fallando")
                elif stats['no_honda'] > total * 0.5:
                    self.logger.warning("   - PROBLEMA: Los anuncios no son de Honda")
                elif stats['no_cb125r'] > total * 0.5:
                    self.logger.warning("   - PROBLEMA: Los anuncios no son CB125R")


# Función principal
def run_cb125r_scraper():
    """Función principal para ejecutar el scraper de CB125R"""
    try:
        print("="*80)
        print("    SCRAPER CB125R - VERSION FUNCIONAL")
        print("="*80)
        print(" Objetivo: Conseguir datos reales de CB125R")
        print(" Validación flexible + debug extenso")
        print()
        
        scraper = ScraperCB125R()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"\n SCRAPING EXITOSO: {len(df_results)} motos encontradas")
            
            # Mostrar muestra
            print(f"\n MUESTRA DE RESULTADOS:")
            for i, (_, moto) in enumerate(df_results.head(5).iterrows(), 1):
                titulo = moto['Titulo'][:40] if len(moto['Titulo']) > 40 else moto['Titulo']
                print(f"   {i}. {titulo} | {moto['Precio']} | {moto['Kilometraje']} | {moto['Año']}")
            
            return df_results
        else:
            print("\n NO SE ENCONTRARON MOTOS CB125R")
            print(" Revisar logs de validación arriba")
            return df_results
            
    except Exception as e:
        print(f"\n ERROR: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_cb125r_scraper()
    if results is not None and not results.empty:
        print(f"\n ÉXITO: {len(results)} motos CB125R extraídas")
    else:
        print(f"\n REVISAR: Logs de debug para identificar problema")
