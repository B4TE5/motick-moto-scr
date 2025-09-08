"""
================================================================================
                            SCRAPER KYMCO AGILITY125                   
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

class ScraperAGILITY125(BaseScraper):
    """Scraper específico para Kymco Agility125"""
    
    def __init__(self):
        super().__init__('agility125')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bagility[\-\s]*125\b',
            r'\bagility\s*125\b',
            r'\bagility\s*[\-\.\/]\s*125\b',
            r'\ba[\-\s]*g[\-\s]*i[\-\s]*l[\-\s]*i[\-\s]*t[\-\s]*y[\-\s]*125\b'
        ]
        
        self.exclude_patterns = [
            r'\bagility[\-\s]*50\b',   # Agility 50
            r'\bagility[\-\s]*16\b',   # Agility 16
            r'\bagility[\-\s]*200\b',  # Agility 200
            r'\bagility[\-\s]*150\b',  # Agility 150
            r'\bpeople\b',             # People (otro modelo Kymco)
            r'\bsuper[\-\s]*8\b',      # Super 8
            r'\bdowntown\b',           # Downtown
            r'\bxciting\b',            # Xciting
            r'\bmyroad\b'              # MyRoad
        ]
        
        self.logger.info(f"Scraper Agility125 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Kymco Agility125 - VERSIÓN EXTENDIDA"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # 1. BÚSQUEDAS PRINCIPALES BÁSICAS
        base_queries = [
            "kymco%20agility125", "kymco%20agility%20125", "agility125", "agility%20125",
            "kymco%20agility", "scooter%20kymco%20agility125", "scooter%20kymco%20agility",
            "kymco%20agility%20scooter", "agility%20kymco", "scooter%20agility125"
        ]
        
        # 2. BÚSQUEDAS POR AÑOS ESPECÍFICOS (2014-2024)
        year_queries = []
        for year in range(2014, 2025):
            year_queries.extend([
                f"kymco%20agility125%20{year}",
                f"agility125%20{year}",
                f"kymco%20agility%20{year}",
                f"scooter%20kymco%20{year}%20agility",
                f"agility%20{year}%20kymco"
            ])
        
        # 3. BÚSQUEDAS POR RANGOS DE PRECIOS
        price_ranges = [
            (min_price, min_price + 400),
            (min_price + 300, min_price + 800),
            (min_price + 600, min_price + 1200),
            (min_price + 1000, max_price)
        ]
        
        price_queries = []
        for min_p, max_p in price_ranges:
            price_queries.extend([
                f"agility125&min_sale_price={min_p}&max_sale_price={max_p}",
                f"kymco%20agility&min_sale_price={min_p}&max_sale_price={max_p}",
                f"scooter%20kymco&min_sale_price={min_p}&max_sale_price={max_p}",
                f"scooter%20economico&min_sale_price={min_p}&max_sale_price={max_p}"
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
            ("malaga", "36.7196", "-4.4214")
        ]
        
        regional_queries = []
        for city, lat, lng in regions:
            regional_queries.extend([
                f"agility125&latitude={lat}&longitude={lng}&distance=50000",
                f"kymco%20agility&latitude={lat}&longitude={lng}&distance=50000",
                f"scooter%20kymco&latitude={lat}&longitude={lng}&distance=75000",
                f"scooter%20economico&latitude={lat}&longitude={lng}&distance=75000"
            ])
        
        # 5. BÚSQUEDAS CON TÉRMINOS RELACIONADOS
        related_queries = [
            "scooter%20125%20kymco", "kymco%20125%20scooter", "scooter%20economico%20kymco",
            "kymco%20scooter%20125", "moto%20automatica%20kymco", "scooter%20automatico%20kymco",
            "kymco%20agility%20original", "agility%20kymco%20oficial", "scooter%20kymco%20nuevo",
            "scooter%20kymco%20segunda%20mano", "agility%20segunda%20mano", "kymco%20agility%20usado",
            "scooter%20barato%20kymco", "kymco%20economico", "scooter%20kymco%20barato"
        ]
        
        # 6. BÚSQUEDAS CON ERRORES ORTOGRÁFICOS COMUNES
        misspelling_queries = [
            "kimco%20agility125", "kymco%20agiliti125", "kymco%20agility12", "kimco%20agility%20125",
            "kymco%20agility%20modelo%20125", "scuter%20kymco%20agility", "escoter%20kymco%20125",
            "kymco%20agilty125", "agility%20125%20kimco", "kymco%20agiliyt"
        ]
        
        # 7. BÚSQUEDAS CON FILTROS ESPECÍFICOS
        specific_queries = [
            "agility125%20particular", "agility125%20concesionario", "kymco%20agility%20taller",
            "agility125%20garantia", "kymco%20agility%20financiacion", "scooter%20kymco%20ocasion",
            "agility125%20barato", "kymco%20agility%20economico", "scooter%20agility%20oferta"
        ]
        
        # 8. BÚSQUEDAS POR CARACTERÍSTICAS TÉCNICAS
        technical_queries = [
            "scooter%20125%20automatico%20kymco", "scooter%20125cc%20kymco", "kymco%20125%20automatico",
            "scooter%20kymco%20cvt", "scooter%20urbano%20kymco", "kymco%20125%20urbano",
            "scooter%20economico%20125", "moto%20125%20automatica%20kymco"
        ]
        
        # 9. BÚSQUEDAS DE VARIANTES ESPECÍFICAS
        variant_queries = [
            "agility%20plus", "agility%20city", "kymco%20agility%20plus", "kymco%20agility%20city",
            "agility%20125%20plus", "agility%20125%20city", "agility%20naked", "agility%20carry"
        ]
        
        # 10. BÚSQUEDAS CON TÉRMINOS DE MERCADO DE SEGUNDA MANO
        market_queries = [
            "scooter%20segunda%20mano%20kymco", "kymco%20ocasion", "agility%20ocasion",
            "scooter%20usado%20kymco", "kymco%20segunda%20mano", "agility%20segunda%20mano",
            "scooter%20seminuevo%20kymco", "kymco%20seminuevo"
        ]
        
        # 11. COMBINAR TODAS LAS QUERIES
        all_base_queries = (base_queries + year_queries + related_queries + 
                           misspelling_queries + specific_queries + technical_queries + 
                           variant_queries + market_queries)
        
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
        
        # 12. BÚSQUEDAS ESPECÍFICAS SIN FILTRO DE PRECIO (para encontrar ofertas)
        no_price_queries = ["agility125", "kymco%20agility", "scooter%20kymco%20125"]
        for query in no_price_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=price_low_to_high")
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&order_by=newest")
        
        # 13. ELIMINAR DUPLICADOS MANTENIENDO ORDEN
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"URLs generadas para Agility125: {len(unique_urls)}")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validar si los datos corresponden realmente a una Kymco Agility125
        
        Args:
            moto_data: Diccionario con datos de la moto
            
        Returns:
            bool: True si es válida Agility125
        """
        try:
            titulo = moto_data.get('Título', '').lower()
            descripcion = moto_data.get('Descripcion', '').lower()
            combined_text = f"{titulo} {descripcion}"
            
            # PASO 1: Verificar que sea Kymco
            kymco_found = self._is_kymco_brand(combined_text)
            if not kymco_found:
                self.logger.debug("No es Kymco")
                return False
            
            # PASO 2: Verificar modelo Agility125 específico
            agility125_found = self._is_agility125_model(combined_text)
            if not agility125_found:
                self.logger.debug("No es Agility125")
                return False
            
            # PASO 3: Excluir otros modelos Agility o Kymco
            if self._is_excluded_model(combined_text):
                self.logger.debug("Es otro modelo Kymco excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug("Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug("Año fuera de rango")
                return False
            
            self.logger.debug(f"Agility125 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False
    
    def _is_kymco_brand(self, text: str) -> bool:
        """Verificar si es marca Kymco"""
        kymco_patterns = [
            r'\bkymco\b',
            r'\bkimco\b',  # Error común
            r'\bkimko\b',  # Error común
        ]
        
        for pattern in kymco_patterns:
            if re.search(pattern, text):
                return True
        
        # Si encuentra Agility 125 pero no marca, asumir que es Kymco
        agility_patterns = [
            r'\bagility[\-\s]*125\b',
            r'\bagility\s*125\b'
        ]
        
        for pattern in agility_patterns:
            if re.search(pattern, text):
                return True
        
        return False
    
    def _is_agility125_model(self, text: str) -> bool:
        """Verificar si es modelo Agility125 específico"""
        for pattern in self.model_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_excluded_model(self, text: str) -> bool:
        """Verificar si es un modelo excluido (Agility50, People, etc.)"""
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

def run_agility125_scraper():
    """Función principal para ejecutar el scraper de Agility125"""
    try:
        scraper = ScraperAGILITY125()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"Scraping Agility125 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print("No se encontraron motos Agility125")
            return df_results
            
    except Exception as e:
        print(f"Error en scraper Agility125: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_agility125_scraper()
    if results is not None and not results.empty:
        print(f"\nPrimeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")
