"""
================================================================================
                            SCRAPER YAMAHA MT-07                  
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

class ScraperMT07(BaseScraper):
    """Scraper específico para Yamaha MT-07"""
    
    def __init__(self):
        super().__init__('mt07')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bmt[\-\s]*07\b',
            r'\bmt[\-\s]*7\b',
            r'\bmt\s*07\b',
            r'\bmt\s*7\b',
            r'\bmt\s*[\-\.\/]\s*07\b',
            r'\bmt\s*[\-\.\/]\s*7\b',
            r'\bm[\-\s]*t[\-\s]*07\b',
            r'\bm[\-\s]*t[\-\s]*7\b'
        ]
        
        self.exclude_patterns = [
            r'\bmt[\-\s]*03\b',   # MT-03
            r'\bmt[\-\s]*09\b',   # MT-09
            r'\bmt[\-\s]*10\b',   # MT-10
            r'\bmt[\-\s]*125\b',  # MT-125
            r'\bmt[\-\s]*15\b',   # MT-15
            r'\btracer\b',        # Tracer (basada en MT pero diferente)
            r'\bxmax\b',          # XMAX
            r'\btmax\b',          # TMAX
            r'\bfz[\-\s]*\d+\b'   # FZ series
        ]
        
        self.logger.info(f"Scraper MT-07 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Yamaha MT-07 - VERSIÓN EXTENDIDA"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # 1. BÚSQUEDAS PRINCIPALES BÁSICAS
        base_queries = [
            "yamaha%20mt07", "yamaha%20mt-07", "yamaha%20mt%2007", "mt07", "mt-07", "mt%2007",
            "yamaha%20mt07%20naked", "naked%20yamaha%20mt07", "yamaha%20mt%207", "mt%207%20yamaha",
            "yamaha%20mt%20serie", "naked%20yamaha%20700", "yamaha%20naked%20mt"
        ]
        
        # 2. BÚSQUEDAS POR AÑOS ESPECÍFICOS (2014-2024)
        year_queries = []
        for year in range(2014, 2025):
            year_queries.extend([
                f"yamaha%20mt07%20{year}",
                f"mt07%20{year}",
                f"yamaha%20mt-07%20{year}",
                f"naked%20yamaha%20{year}%20mt07",
                f"mt%2007%20{year}",
                f"yamaha%20{year}%20mt07"
            ])
        
        # 3. BÚSQUEDAS POR RANGOS DE PRECIOS
        price_ranges = [
            (min_price, min_price + 800),
            (min_price + 600, min_price + 1500),
            (min_price + 1200, min_price + 2000),
            (min_price + 1800, max_price)
        ]
        
        price_queries = []
        for min_p, max_p in price_ranges:
            price_queries.extend([
                f"mt07&min_sale_price={min_p}&max_sale_price={max_p}",
                f"yamaha%20mt07&min_sale_price={min_p}&max_sale_price={max_p}",
                f"naked%20yamaha&min_sale_price={min_p}&max_sale_price={max_p}",
                f"yamaha%20naked&min_sale_price={min_p}&max_sale_price={max_p}"
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
            ("alicante", "38.3452", "-0.4810"),
            ("cordoba", "37.8882", "-4.7794"),
            ("valladolid", "41.6523", "-4.7245")
        ]
        
        regional_queries = []
        for city, lat, lng in regions:
            regional_queries.extend([
                f"mt07&latitude={lat}&longitude={lng}&distance=50000",
                f"yamaha%20mt07&latitude={lat}&longitude={lng}&distance=50000",
                f"naked%20yamaha&latitude={lat}&longitude={lng}&distance=75000",
                f"yamaha%20naked&latitude={lat}&longitude={lng}&distance=75000"
            ])
        
        # 5. BÚSQUEDAS CON TÉRMINOS RELACIONADOS
        related_queries = [
            "naked%20yamaha%20700", "yamaha%20700%20naked", "moto%20naked%20yamaha",
            "yamaha%20mt%20serie", "yamaha%20roadster", "naked%20yamaha%20media",
            "yamaha%20mt07%20original", "mt07%20yamaha%20oficial", "naked%20yamaha%20nuevo",
            "naked%20yamaha%20segunda%20mano", "mt07%20segunda%20mano", "yamaha%20mt07%20usado",
            "moto%20yamaha%20naked", "roadster%20yamaha", "yamaha%20sport%20naked"
        ]
        
        # 6. BÚSQUEDAS CON ERRORES ORTOGRÁFICOS COMUNES
        misspelling_queries = [
            "yamaha%20mt%200%207", "yamaha%20mt%2070", "mt%2070%20yamaha", "yamaha%20mt%207",
            "yamaha%20mt07%20modello", "nakeed%20yamaha%20mt07", "mt%2007%20iamaha",
            "yamaha%20mt%20cero%20siete", "mt%20siete%20yamaha"
        ]
        
        # 7. BÚSQUEDAS CON FILTROS ESPECÍFICOS
        specific_queries = [
            "mt07%20particular", "mt07%20concesionario", "yamaha%20mt07%20taller",
            "mt07%20garantia", "yamaha%20mt07%20financiacion", "naked%20yamaha%20ocasion",
            "mt07%20abs", "yamaha%20mt07%20abs", "naked%20yamaha%20abs"
        ]
        
        # 8. BÚSQUEDAS POR CARACTERÍSTICAS TÉCNICAS
        technical_queries = [
            "yamaha%20700%20bicilindrica", "naked%20700cc%20yamaha", "yamaha%20cp2%20motor",
            "yamaha%20crossplane%20mt07", "naked%20yamaha%20cp2", "yamaha%20bicilindrica%20naked"
        ]
        
        # 9. COMBINAR TODAS LAS QUERIES
        all_base_queries = (base_queries + year_queries + related_queries + 
                           misspelling_queries + specific_queries + technical_queries)
        
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
        
        # 10. BÚSQUEDAS ESPECÍFICAS SIN FILTRO DE PRECIO (para encontrar ofertas)
        no_price_queries = ["mt07", "yamaha%20mt07", "naked%20yamaha%20mt"]
        for query in no_price_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=price_low_to_high")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=newest")
        
        # 11. ELIMINAR DUPLICADOS MANTENIENDO ORDEN
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"URLs generadas para MT-07: {len(unique_urls)}")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validar si los datos corresponden realmente a una Yamaha MT-07
        
        Args:
            moto_data: Diccionario con datos de la moto
            
        Returns:
            bool: True si es válida MT-07
        """
        try:
            titulo = moto_data.get('Título', '').lower()
            descripcion = moto_data.get('Descripcion', '').lower()
            combined_text = f"{titulo} {descripcion}"
            
            # PASO 1: Verificar que sea Yamaha
            yamaha_found = self._is_yamaha_brand(combined_text)
            if not yamaha_found:
                self.logger.debug("No es Yamaha")
                return False
            
            # PASO 2: Verificar modelo MT-07 específico
            mt07_found = self._is_mt07_model(combined_text)
            if not mt07_found:
                self.logger.debug("No es MT-07")
                return False
            
            # PASO 3: Excluir otros modelos MT de Yamaha
            if self._is_excluded_model(combined_text):
                self.logger.debug("Es otro modelo MT de Yamaha excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug("Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug("Año fuera de rango")
                return False
            
            self.logger.debug(f"MT-07 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False
    
    def _is_yamaha_brand(self, text: str) -> bool:
        """Verificar si es marca Yamaha"""
        yamaha_patterns = [
            r'\byamaha\b',
            r'\byamha\b',   # Error común
            r'\bymaha\b',   # Error común
        ]
        
        for pattern in yamaha_patterns:
            if re.search(pattern, text):
                return True
        
        # Si encuentra MT-07 pero no marca, asumir que es Yamaha
        mt07_patterns = [
            r'\bmt[\-\s]*07\b',
            r'\bmt[\-\s]*7\b'
        ]
        
        for pattern in mt07_patterns:
            if re.search(pattern, text):
                return True
        
        return False
    
    def _is_mt07_model(self, text: str) -> bool:
        """Verificar si es modelo MT-07 específico"""
        for pattern in self.model_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_excluded_model(self, text: str) -> bool:
        """Verificar si es un modelo excluido (MT-03, MT-09, etc.)"""
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

def run_mt07_scraper():
    """Función principal para ejecutar el scraper de MT-07"""
    try:
        scraper = ScraperMT07()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"Scraping MT-07 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print("No se encontraron motos MT-07")
            return df_results
            
    except Exception as e:
        print(f"Error en scraper MT-07: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_mt07_scraper()
    if results is not None and not results.empty:
        print(f"\nPrimeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")
