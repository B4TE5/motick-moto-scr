"""
================================================================================
                          SCRAPER KAWASAKI Z900                  
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

class ScraperZ900(BaseScraper):
    """Scraper específico para Kawasaki Z900"""
    
    def __init__(self):
        super().__init__('z900')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bz[\-\s]*900\b',
            r'\bz\s*900\b',
            r'\bz\s*[\-\.\/]\s*900\b',
            r'\bz[\-\s]*9[\-\s]*0[\-\s]*0\b',
            r'\bz900[\-\s]*rs\b',     # Z900RS variante
            r'\bz900[\-\s]*e\b'       # Z900E variante
        ]
        
        self.exclude_patterns = [
            r'\bz[\-\s]*800\b',   # Z800
            r'\bz[\-\s]*650\b',   # Z650
            r'\bz[\-\s]*1000\b',  # Z1000
            r'\bz[\-\s]*300\b',   # Z300
            r'\bz[\-\s]*250\b',   # Z250
            r'\bzx[\-\s]*\d+\b',  # ZX series (deportivas)
            r'\ber[\-\s]*6\b',    # ER-6 (diferente)
            r'\bninja\b'          # Ninja series
        ]
        
        self.logger.info(f"Scraper Z900 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Kawasaki Z900 - VERSIÓN EXTENDIDA"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # 1. BÚSQUEDAS PRINCIPALES BÁSICAS
        base_queries = [
            "kawasaki%20z900", "kawasaki%20z%20900", "z900", "z%20900",
            "kawasaki%20z900%20naked", "naked%20kawasaki%20z900", "kawasaki%20z900rs",
            "z900%20rs", "kawasaki%20z%20900%20rs", "z900rs", "naked%20kawasaki%20900"
        ]
        
        # 2. BÚSQUEDAS POR AÑOS ESPECÍFICOS (2017-2024)
        year_queries = []
        for year in range(2017, 2025):
            year_queries.extend([
                f"kawasaki%20z900%20{year}",
                f"z900%20{year}",
                f"kawasaki%20z%20900%20{year}",
                f"naked%20kawasaki%20{year}%20z900",
                f"z900rs%20{year}",
                f"kawasaki%20{year}%20z900"
            ])
        
        # 3. BÚSQUEDAS POR RANGOS DE PRECIOS
        price_ranges = [
            (min_price, min_price + 1000),
            (min_price + 800, min_price + 2000),
            (min_price + 1500, min_price + 3000),
            (min_price + 2500, max_price)
        ]
        
        price_queries = []
        for min_p, max_p in price_ranges:
            price_queries.extend([
                f"z900&min_sale_price={min_p}&max_sale_price={max_p}",
                f"kawasaki%20z900&min_sale_price={min_p}&max_sale_price={max_p}",
                f"naked%20kawasaki&min_sale_price={min_p}&max_sale_price={max_p}",
                f"kawasaki%20naked&min_sale_price={min_p}&max_sale_price={max_p}"
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
            ("valladolid", "41.6523", "-4.7245"),
            ("santander", "43.4623", "-3.8099"),
            ("gijon", "43.5322", "-5.6611")
        ]
        
        regional_queries = []
        for city, lat, lng in regions:
            regional_queries.extend([
                f"z900&latitude={lat}&longitude={lng}&distance=50000",
                f"kawasaki%20z900&latitude={lat}&longitude={lng}&distance=50000",
                f"naked%20kawasaki&latitude={lat}&longitude={lng}&distance=75000",
                f"kawasaki%20naked&latitude={lat}&longitude={lng}&distance=75000"
            ])
        
        # 5. BÚSQUEDAS CON TÉRMINOS RELACIONADOS
        related_queries = [
            "naked%20kawasaki%20900", "kawasaki%20900%20naked", "moto%20naked%20kawasaki",
            "kawasaki%20z%20serie", "kawasaki%20roadster", "naked%20kawasaki%20alta",
            "kawasaki%20z900%20original", "z900%20kawasaki%20oficial", "naked%20kawasaki%20nuevo",
            "naked%20kawasaki%20segunda%20mano", "z900%20segunda%20mano", "kawasaki%20z900%20usado",
            "moto%20kawasaki%20naked", "roadster%20kawasaki", "kawasaki%20sport%20naked",
            "kawasaki%20z900%20performance", "z900%20sugomi"
        ]
        
        # 6. BÚSQUEDAS CON ERRORES ORTOGRÁFICOS COMUNES
        misspelling_queries = [
            "kawasaki%20z%209%200%200", "kawasaki%20z%20900", "z%20900%20kawasaki",
            "kawasaky%20z900", "kawasaki%20z900%20modello", "nakeed%20kawasaki%20z900",
            "z%20900%20kawasaky", "kawasaki%20z%20novecientos", "z%20novecientos%20kawasaki"
        ]
        
        # 7. BÚSQUEDAS CON FILTROS ESPECÍFICOS
        specific_queries = [
            "z900%20particular", "z900%20concesionario", "kawasaki%20z900%20taller",
            "z900%20garantia", "kawasaki%20z900%20financiacion", "naked%20kawasaki%20ocasion",
            "z900%20abs", "kawasaki%20z900%20abs", "naked%20kawasaki%20abs",
            "z900%20quickshifter", "z900%20se"
        ]
        
        # 8. BÚSQUEDAS POR CARACTERÍSTICAS TÉCNICAS
        technical_queries = [
            "kawasaki%20900%20cuatro%20cilindros", "naked%20900cc%20kawasaki", "kawasaki%20z900%20inline4",
            "kawasaki%20z900%20sugomi", "naked%20kawasaki%20948cc", "kawasaki%20cuatro%20cilindros%20naked",
            "z900%20performance", "kawasaki%20ninja%20z900", "z900%20streetfighter"
        ]
        
        # 9. BÚSQUEDAS DE VARIANTES ESPECÍFICAS
        variant_queries = [
            "z900rs", "z900%20rs", "kawasaki%20z900rs", "kawasaki%20z900%20rs",
            "z900%20retro", "kawasaki%20z900%20retro", "z900%20classic", "z900%20cafe",
            "z900se", "z900%20se", "kawasaki%20z900se"
        ]
        
        # 10. COMBINAR TODAS LAS QUERIES
        all_base_queries = (base_queries + year_queries + related_queries + 
                           misspelling_queries + specific_queries + technical_queries + variant_queries)
        
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
        
        # 11. BÚSQUEDAS ESPECÍFICAS SIN FILTRO DE PRECIO (para encontrar ofertas)
        no_price_queries = ["z900", "kawasaki%20z900", "naked%20kawasaki%20z"]
        for query in no_price_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=price_low_to_high")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=newest")
        
        # 12. ELIMINAR DUPLICADOS MANTENIENDO ORDEN
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"URLs generadas para Z900: {len(unique_urls)}")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validar si los datos corresponden realmente a una Kawasaki Z900
        
        Args:
            moto_data: Diccionario con datos de la moto
            
        Returns:
            bool: True si es válida Z900
        """
        try:
            titulo = moto_data.get('Título', '').lower()
            descripcion = moto_data.get('Descripcion', '').lower()
            combined_text = f"{titulo} {descripcion}"
            
            # PASO 1: Verificar que sea Kawasaki
            kawasaki_found = self._is_kawasaki_brand(combined_text)
            if not kawasaki_found:
                self.logger.debug("No es Kawasaki")
                return False
            
            # PASO 2: Verificar modelo Z900 específico
            z900_found = self._is_z900_model(combined_text)
            if not z900_found:
                self.logger.debug("No es Z900")
                return False
            
            # PASO 3: Excluir otros modelos Z de Kawasaki
            if self._is_excluded_model(combined_text):
                self.logger.debug("Es otro modelo Z de Kawasaki excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug("Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug("Año fuera de rango")
                return False
            
            self.logger.debug(f"Z900 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False
    
    def _is_kawasaki_brand(self, text: str) -> bool:
        """Verificar si es marca Kawasaki"""
        kawasaki_patterns = [
            r'\bkawasaki\b',
            r'\bkawa\b',      # Abreviación común
            r'\bkawasaky\b',  # Error común
        ]
        
        for pattern in kawasaki_patterns:
            if re.search(pattern, text):
                return True
        
        # Si encuentra Z900 pero no marca, asumir que es Kawasaki
        z900_patterns = [
            r'\bz[\-\s]*900\b',
            r'\bz\s*900\b'
        ]
        
        for pattern in z900_patterns:
            if re.search(pattern, text):
                return True
        
        return False
    
    def _is_z900_model(self, text: str) -> bool:
        """Verificar si es modelo Z900 específico"""
        for pattern in self.model_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_excluded_model(self, text: str) -> bool:
        """Verificar si es un modelo excluido (Z800, Z650, etc.)"""
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

def run_z900_scraper():
    """Función principal para ejecutar el scraper de Z900"""
    try:
        scraper = ScraperZ900()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"Scraping Z900 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print("No se encontraron motos Z900")
            return df_results
            
    except Exception as e:
        print(f"Error en scraper Z900: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_z900_scraper()
    if results is not None and not results.empty:
        print(f"\nPrimeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")
