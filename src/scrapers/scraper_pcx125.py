"""
================================================================================
                            SCRAPER HONDA PCX125                  
================================================================================

Autor: Carlos Peraza
Versión: 2.0
Fecha: Agosto 2025

================================================================================
"""

import re
import logging
from typing import Dict, List, Optional
from scrapers.base_scraper import BaseScraper

class ScraperPCX125(BaseScraper):
    """Scraper específico para Honda PCX125"""
    
    def __init__(self):
        super().__init__('pcx125')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bpcx[\-\s]*125\b',
            r'\bpcx\s*125\b',
            r'\bpcx\s*[\-\.\/]\s*125\b',
            r'\bp[\-\s]*c[\-\s]*x[\-\s]*125\b'
        ]
        
        self.exclude_patterns = [
            r'\bpcx[\-\s]*150\b',  # PCX150
            r'\bpcx[\-\s]*160\b',  # PCX160
            r'\bforza\b',          # Honda Forza (diferente)
            r'\bsh[\-\s]*\d+\b',   # SH125, SH150, etc.
            r'\bdylan\b',          # Dylan (diferente marca)
            r'\bxmax\b'            # XMAX (Yamaha)
        ]
        
        self.logger.info(f"Scraper PCX125 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Honda PCX125 - VERSIÓN EXTENDIDA"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # 1. BÚSQUEDAS PRINCIPALES BÁSICAS
        base_queries = [
            "honda%20pcx125", "honda%20pcx%20125", "pcx125", "pcx%20125",
            "honda%20pcx", "scooter%20honda%20pcx125", "scooter%20honda%20pcx",
            "honda%20pcx%20scooter", "pcx%20honda", "scooter%20pcx125"
        ]
        
        # 2. BÚSQUEDAS POR AÑOS ESPECÍFICOS (2016-2024)
        year_queries = []
        for year in range(2016, 2025):
            year_queries.extend([
                f"honda%20pcx125%20{year}",
                f"pcx125%20{year}",
                f"honda%20pcx%20{year}",
                f"scooter%20honda%20{year}%20pcx"
            ])
        
        # 3. BÚSQUEDAS POR RANGOS DE PRECIOS
        price_ranges = [
            (min_price, min_price + 500),
            (min_price + 400, min_price + 1000),
            (min_price + 800, min_price + 1500),
            (min_price + 1200, max_price)
        ]
        
        price_queries = []
        for min_p, max_p in price_ranges:
            price_queries.extend([
                f"pcx125&min_sale_price={min_p}&max_sale_price={max_p}",
                f"honda%20pcx&min_sale_price={min_p}&max_sale_price={max_p}",
                f"scooter%20honda&min_sale_price={min_p}&max_sale_price={max_p}"
            ])
        
        # 4. BÚSQUEDAS POR CIUDADES Y REGIONES
        regions = [
            ("madrid", "40.4168", "-3.7038"),
            ("barcelona", "41.3851", "2.1734"),
            ("valencia", "39.4699", "-0.3763"),
            ("sevilla", "37.3891", "-5.9845"),
            ("bilbao", "43.2630", "-2.9350"),
            ("zaragoza", "41.6488", "-0.8891"),
            ("murcia", "37.9922", "-1.1307"),
            ("palma", "39.5696", "2.6502"),
            ("las%20palmas", "28.1248", "-15.4300"),
            ("alicante", "38.3452", "-0.4810")
        ]
        
        regional_queries = []
        for city, lat, lng in regions:
            regional_queries.extend([
                f"pcx125&latitude={lat}&longitude={lng}&distance=50000",
                f"honda%20pcx&latitude={lat}&longitude={lng}&distance=50000",
                f"scooter%20honda&latitude={lat}&longitude={lng}&distance=75000"
            ])
        
        # 5. BÚSQUEDAS CON TÉRMINOS RELACIONADOS
        related_queries = [
            "scooter%20125%20honda", "honda%20125%20scooter", "scooter%20urbano%20honda",
            "honda%20scooter%20125", "moto%20automatica%20honda", "scooter%20automatico",
            "honda%20pcx%20original", "pcx%20honda%20oficial", "scooter%20honda%20nuevo",
            "scooter%20honda%20segunda%20mano", "pcx%20segunda%20mano", "honda%20pcx%20usado"
        ]
        
        # 6. BÚSQUEDAS CON ERRORES ORTOGRÁFICOS COMUNES
        misspelling_queries = [
            "honda%20pcx12", "honda%20pc%20125", "honda%20pxc125", "pcx%20honda%20125",
            "honda%20pcx%20modelo%20125", "scuter%20honda%20pcx", "escoter%20honda%20125"
        ]
        
        # 7. BÚSQUEDAS CON FILTROS ESPECÍFICOS
        specific_queries = [
            "pcx125%20particular", "pcx125%20concesionario", "honda%20pcx%20taller",
            "pcx125%20garantia", "honda%20pcx%20financiacion", "scooter%20honda%20ocasion"
        ]
        
        # 8. COMBINAR TODAS LAS QUERIES
        all_base_queries = (base_queries + year_queries + related_queries + 
                           misspelling_queries + specific_queries)
        
        # GENERAR URLS PRINCIPALES
        for query in all_base_queries:
            # URL básica
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
            
            # URL ordenada por más recientes
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest")
            
            # URL ordenada por precio
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&order_by=price_low_to_high")
        
        # AÑADIR BÚSQUEDAS POR RANGOS DE PRECIOS
        for query in price_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=newest")
        
        # AÑADIR BÚSQUEDAS REGIONALES
        for query in regional_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=newest")
        
        # 9. BÚSQUEDAS ESPECÍFICAS SIN FILTRO DE PRECIO (para encontrar ofertas)
        no_price_queries = ["pcx125", "honda%20pcx", "scooter%20honda%20125"]
        for query in no_price_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=price_low_to_high")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=newest")
        
        # 10. ELIMINAR DUPLICADOS MANTENIENDO ORDEN
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"URLs generadas para PCX125: {len(unique_urls)}")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validar si los datos corresponden realmente a una Honda PCX125
        
        Args:
            moto_data: Diccionario con datos de la moto
            
        Returns:
            bool: True si es válida PCX125
        """
        try:
            titulo = moto_data.get('Título', '').lower()
            descripcion = moto_data.get('Descripcion', '').lower()
            combined_text = f"{titulo} {descripcion}"
            
            # PASO 1: Verificar que sea Honda
            honda_found = self._is_honda_brand(combined_text)
            if not honda_found:
                self.logger.debug("No es Honda")
                return False
            
            # PASO 2: Verificar modelo PCX125 específico
            pcx125_found = self._is_pcx125_model(combined_text)
            if not pcx125_found:
                self.logger.debug("No es PCX125")
                return False
            
            # PASO 3: Excluir otros modelos PCX o scooters
            if self._is_excluded_model(combined_text):
                self.logger.debug("Es otro modelo PCX o scooter excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug("Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug("Año fuera de rango")
                return False
            
            self.logger.debug(f"PCX125 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False
    
    def _is_honda_brand(self, text: str) -> bool:
        """Verificar si es marca Honda"""
        honda_patterns = [
            r'\bhonda\b',
            r'\bhond\b',  # Error común
        ]
        
        for pattern in honda_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_pcx125_model(self, text: str) -> bool:
        """Verificar si es modelo PCX125 específico"""
        for pattern in self.model_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_excluded_model(self, text: str) -> bool:
        """Verificar si es un modelo excluido (PCX150, Forza, etc.)"""
        for pattern in self.exclude_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_valid_price_range(self, precio_text: str) -> bool:
        """Validar si el precio está en el rango esperado"""
        if not precio_text:
            return True  # Aceptar si no hay precio
        
        try:
            price_numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', ''))
            if price_numbers:
                price = int(price_numbers[0])
                min_price = self.modelo_config['precio_min']
                max_price = self.modelo_config['precio_max']
                
                return min_price <= price <= max_price
        except:
            pass
        
        return True  # Aceptar si no se puede parsear
    
    def _is_valid_year_range(self, año_text: str) -> bool:
        """Validar si el año está en el rango esperado"""
        if not año_text:
            return True  # Aceptar si no hay año
        
        try:
            year_match = re.search(r'(20[0-9]{2})', str(año_text))
            if year_match:
                year = int(year_match.group(1))
                min_year = self.modelo_config['año_min']
                max_year = self.modelo_config['año_max']
                
                return min_year <= year <= max_year
        except:
            pass
        
        return True  # Aceptar si no se puede parsear

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_pcx125_scraper():
    """Función principal para ejecutar el scraper de PCX125"""
    try:
        scraper = ScraperPCX125()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"Scraping PCX125 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print("No se encontraron motos PCX125")
            return df_results
            
    except Exception as e:
        print(f"Error en scraper PCX125: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_pcx125_scraper()
    if results is not None and not results.empty:
        print(f"\nPrimeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")
