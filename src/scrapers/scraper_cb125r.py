"""
================================================================================
                      SCRAPER CB125R CORREGIDO Y DEBUGEADO                     
================================================================================

Autor: Carlos Peraza
Version: 4.0 
Fecha: Septiembre 2025

================================================================================
"""

import re
import logging
from typing import Dict, List, Optional
from scrapers.base_scraper import BaseScraper

class ScraperCB125R(BaseScraper):
    """Scraper CB125R CORREGIDO con debug extenso"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Contadores para debug
        self.debug_stats = {
            'total_procesados': 0,
            'titulo_vacio': 0,
            'honda_no_encontrada': 0,
            'cb125r_no_encontrado': 0,
            'modelo_excluido': 0,
            'precio_invalido': 0,
            'año_invalido': 0,
            'validaciones_exitosas': 0
        }
        
        self.logger.info(f"Scraper CB125R CORREGIDO inicializado - Debug ACTIVADO")
    
    def get_search_urls(self) -> List[str]:
        """URLs optimizadas y focalizadas para CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        # URLs MAS ESPECIFICAS Y EFECTIVAS
        urls = [
            # Búsquedas principales más específicas
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb%20125%20r&min_sale_price={min_price}&max_sale_price={max_price}",
            
            # Con ordenamiento
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}&order_by=price_low_to_high",
            
            # Variantes comunes
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%20moto&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=moto%20honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            
            # Por años más comunes
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%202020&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%202021&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r%202022&min_sale_price={min_price}&max_sale_price={max_price}",
            
            # Regiones principales
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}&latitude=40.4168&longitude=-3.7038&distance=100000",  # Madrid
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}&latitude=41.3851&longitude=2.1734&distance=100000",   # Barcelona
            
            # Sin filtros de precio (para encontrar gangas)
            "https://es.wallapop.com/app/search?keywords=honda%20cb125r",
            "https://es.wallapop.com/app/search?keywords=cb125r%20honda",
            "https://es.wallapop.com/app/search?keywords=honda%20cb%20125%20r",
            
            # Términos relacionados
            f"https://es.wallapop.com/app/search?keywords=honda%20deportiva%20125&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=moto%20125%20honda&min_sale_price={min_price}&max_sale_price={max_price}",
            
            # Búsquedas más amplias para capturar variantes
            f"https://es.wallapop.com/app/search?keywords=honda%20125%20cb&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=cb%20125%20honda&min_sale_price={min_price}&max_sale_price={max_price}",
        ]
        
        self.logger.info(f"Generadas {len(urls)} URLs optimizadas para CB125R")
        return urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validación CORREGIDA y más flexible con debug extenso
        """
        self.debug_stats['total_procesados'] += 1
        
        try:
            # CORREGIDO: Usar 'Titulo' (sin tilde) consistentemente
            titulo = moto_data.get('Titulo', '').lower().strip()
            precio = moto_data.get('Precio', '')
            año = moto_data.get('Año', '')
            
            # DEBUG: Mostrar datos extraídos
            if self.debug_stats['total_procesados'] <= 5:  # Mostrar primeros 5
                self.logger.info(f"[DEBUG {self.debug_stats['total_procesados']}] Validando:")
                self.logger.info(f"  Titulo: '{titulo}'")
                self.logger.info(f"  Precio: '{precio}'")
                self.logger.info(f"  Año: '{año}'")
            
            # PASO 1: Verificar que hay título
            if not titulo or titulo == "sin titulo":
                self.debug_stats['titulo_vacio'] += 1
                self.logger.debug(f"Rechazado: título vacío")
                return False
            
            # PASO 2: Verificar Honda (más flexible)
            honda_keywords = ['honda', 'hond']  # Incluir errores comunes
            honda_found = any(keyword in titulo for keyword in honda_keywords)
            
            if not honda_found:
                self.debug_stats['honda_no_encontrada'] += 1
                self.logger.debug(f"Rechazado: Honda no encontrada en '{titulo}'")
                return False
            
            # PASO 3: Verificar CB125R (MÁS FLEXIBLE)
            cb125r_patterns = [
                'cb125r', 'cb 125 r', 'cb-125-r', 'cb 125r', 'cb125 r',
                'cb125-r', 'cb 125-r', 'cb-125 r',  # Variantes con guiones
                'cb 125', 'cb125'  # Patrones más amplios
            ]
            
            cb125r_found = any(pattern in titulo for pattern in cb125r_patterns)
            
            if not cb125r_found:
                self.debug_stats['cb125r_no_encontrado'] += 1
                self.logger.debug(f"Rechazado: CB125R no encontrado en '{titulo}'")
                # Continuar para ver si podemos validar por precio
            
            # PASO 4: Excluir otros modelos CB
            excluded_models = ['cb125f', 'cb250r', 'cb500r', 'cbr125', 'cbr']
            excluded = any(model in titulo for model in excluded_models)
            
            if excluded:
                self.debug_stats['modelo_excluido'] += 1
                self.logger.debug(f"Rechazado: Modelo excluido encontrado en '{titulo}'")
                return False
            
            # PASO 5: Validación por precio (más flexible)
            precio_valido = self._validate_price_flexible(precio)
            if not precio_valido:
                self.debug_stats['precio_invalido'] += 1
                self.logger.debug(f"Rechazado: Precio inválido '{precio}'")
            
            # PASO 6: Validación por año (más flexible)
            año_valido = self._validate_year_flexible(año)
            if not año_valido and año != "No especificado":
                self.debug_stats['año_invalido'] += 1
                self.logger.debug(f"Rechazado: Año inválido '{año}'")
            
            # DECISIÓN FINAL (más flexible):
            # Aceptar si:
            # 1. Tiene Honda Y CB125R en título
            # 2. O tiene Honda Y precio válido en rango CB125R
            if cb125r_found:
                self.debug_stats['validaciones_exitosas'] += 1
                self.logger.info(f"✓ ACEPTADO por CB125R en título: '{titulo[:50]}'")
                return True
            elif honda_found and precio_valido:
                # Validación por precio para Honda genérica
                if self._is_cb125r_price_range(precio):
                    self.debug_stats['validaciones_exitosas'] += 1
                    self.logger.info(f"✓ ACEPTADO por Honda + precio CB125R: '{titulo[:50]}' - {precio}")
                    return True
            
            self.logger.debug(f"Rechazado: No cumple criterios mínimos")
            return False
            
        except Exception as e:
            self.logger.error(f"Error en validación: {e}")
            return False
    
    def _validate_price_flexible(self, precio_text: str) -> bool:
        """Validación de precio más flexible"""
        if not precio_text or precio_text == "No especificado":
            return False
        
        try:
            # Extraer números del precio
            numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', ''))
            if numbers:
                price = int(numbers[0])
                # Rango amplio para precios válidos
                return 500 <= price <= 15000
        except:
            pass
        
        return False
    
    def _is_cb125r_price_range(self, precio_text: str) -> bool:
        """Verificar si el precio está en rango específico de CB125R"""
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
    
    def _validate_year_flexible(self, año_text: str) -> bool:
        """Validación de año más flexible"""
        if not año_text or año_text == "No especificado":
            return True  # Aceptar si no hay año
        
        try:
            year_match = re.search(r'(20[0-9]{2})', str(año_text))
            if year_match:
                year = int(year_match.group(1))
                return 2015 <= year <= 2025  # Rango amplio
        except:
            pass
        
        return True  # Aceptar si no se puede parsear
    
    def scrape_model(self) -> pd.DataFrame:
        """Override para añadir logging de debug al final"""
        try:
            result = super().scrape_model()
            
            # MOSTRAR ESTADÍSTICAS DE DEBUG
            self._show_debug_stats()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error en scrape_model: {e}")
            self._show_debug_stats()
            return pd.DataFrame()
    
    def _show_debug_stats(self):
        """Mostrar estadísticas detalladas de debug"""
        stats = self.debug_stats
        total = stats['total_procesados']
        
        self.logger.info("="*60)
        self.logger.info("ESTADÍSTICAS DE VALIDACIÓN CB125R")
        self.logger.info("="*60)
        self.logger.info(f"Total anuncios procesados: {total}")
        
        if total > 0:
            self.logger.info(f"Validaciones exitosas: {stats['validaciones_exitosas']} ({stats['validaciones_exitosas']/total*100:.1f}%)")
            self.logger.info("MOTIVOS DE RECHAZO:")
            self.logger.info(f"  • Título vacío: {stats['titulo_vacio']} ({stats['titulo_vacio']/total*100:.1f}%)")
            self.logger.info(f"  • Honda no encontrada: {stats['honda_no_encontrada']} ({stats['honda_no_encontrada']/total*100:.1f}%)")
            self.logger.info(f"  • CB125R no encontrado: {stats['cb125r_no_encontrado']} ({stats['cb125r_no_encontrado']/total*100:.1f}%)")
            self.logger.info(f"  • Modelo excluido: {stats['modelo_excluido']} ({stats['modelo_excluido']/total*100:.1f}%)")
            self.logger.info(f"  • Precio inválido: {stats['precio_invalido']} ({stats['precio_invalido']/total*100:.1f}%)")
            self.logger.info(f"  • Año inválido: {stats['año_invalido']} ({stats['año_invalido']/total*100:.1f}%)")
        
        self.logger.info("="*60)
        
        # RECOMENDACIONES BASADAS EN ESTADÍSTICAS
        if total > 0 and stats['validaciones_exitosas'] == 0:
            self.logger.warning("  NINGUNA VALIDACIÓN EXITOSA - REVISAR:")
            if stats['titulo_vacio'] > total * 0.5:
                self.logger.warning("   - Extractor de títulos fallando")
            if stats['honda_no_encontrada'] > total * 0.5:
                self.logger.warning("   - Los anuncios encontrados no son de Honda")
            if stats['cb125r_no_encontrado'] > total * 0.3:
                self.logger.warning("   - Los anuncios de Honda no son CB125R")


# FUNCIÓN PRINCIPAL MEJORADA
def run_cb125r_scraper():
    """Función principal con debug mejorado"""
    try:
        print("="*80)
        print("    CB125R SCRAPER - VERSIÓN CORREGIDA CON DEBUG")
        print("="*80)
        print(" CORRECCIONES APLICADAS:")
        print("   • Consistencia en nombres de campos")
        print("   • Validación más flexible")
        print("   • Debug extenso para identificar problemas")
        print("   • Logging detallado de rechazos")
        print()
        
        scraper = ScraperCB125R()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"\n SCRAPING CB125R EXITOSO: {len(df_results)} motos encontradas")
            
            # Mostrar muestra de resultados
            print(f"\n MUESTRA DE RESULTADOS:")
            for i, (_, moto) in enumerate(df_results.head(5).iterrows(), 1):
                titulo = moto['Titulo'][:40] if len(moto['Titulo']) > 40 else moto['Titulo']
                print(f"   {i}. {titulo} | {moto['Precio']} | {moto['Kilometraje']} | {moto['Año']}")
            
            return df_results
        else:
            print("\n NO SE ENCONTRARON MOTOS CB125R")
            print(" Revisar estadísticas de debug arriba para identificar el problema")
            return df_results
            
    except Exception as e:
        print(f"\n ERROR CRÍTICO en scraper CB125R: {e}")
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
        print(f"\n ANÁLISIS: Revisar logs de debug para identificar problemas")
