"""
================================================================================
                    SCRAPER YAMAHA MT-07 - WALLAPOP MOTOS SCRAPER                    
================================================================================

Scraper específico para Yamaha MT-07
Implementa detección ultra precisa del modelo naked versátil

Características:
- Detección específica de Yamaha MT-07 y variantes
- Filtrado optimizado para motos naked versátiles
- URLs optimizadas para este modelo específico
- Validación estricta de marca y modelo

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
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
        
        self.logger.info(f" Scraper MT-07 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Yamaha MT-07"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # URLs principales más efectivas para MT-07
        base_queries = [
            "yamaha%20mt07",
            "yamaha%20mt-07",
            "yamaha%20mt%2007", 
            "mt07",
            "mt-07",
            "mt%2007",
            "yamaha%20mt07%20naked",
            "naked%20yamaha%20mt07",
            "yamaha%20mt07%202014",
            "yamaha%20mt07%202015",
            "yamaha%20mt07%202016",
            "yamaha%20mt07%202017",
            "yamaha%20mt07%202018",
            "yamaha%20mt07%202019",
            "yamaha%20mt07%202020",
            "yamaha%20mt07%202021",
            "yamaha%20mt07%202022",
            "yamaha%20mt07%202023",
            "yamaha%20mt07%202024"
        ]
        
        # Generar URLs con diferentes ordenamientos
        for query in base_queries:
            # URL básica con filtro de precio
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
            
            # URL ordenada por más recientes
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest")
        
        # URLs específicas por regiones principales
        main_regions = ["madrid", "barcelona", "valencia"]
        for region in main_regions:
            query = "yamaha%20mt07"
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&filters_source=search_box&latitude=40.4168&longitude=-3.7038&distance=100000")
        
        # URLs específicas para motos naked Yamaha
        naked_queries = [
            "naked%20yamaha%20700",
            "yamaha%20naked%20mt",
            "moto%20naked%20yamaha",
            "yamaha%20mt%20serie"
        ]
        
        for query in naked_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
        
        # Eliminar duplicados manteniendo orden
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"🔍 {len(unique_urls)} URLs generadas para MT-07")
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
                self.logger.debug(" No es Yamaha")
                return False
            
            # PASO 2: Verificar modelo MT-07 específico
            mt07_found = self._is_mt07_model(combined_text)
            if not mt07_found:
                self.logger.debug(" No es MT-07")
                return False
            
            # PASO 3: Excluir otros modelos MT de Yamaha
            if self._is_excluded_model(combined_text):
                self.logger.debug(" Es otro modelo MT de Yamaha excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug(" Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug(" Año fuera de rango")
                return False
            
            self.logger.debug(f" MT-07 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f" Error validando moto: {e}")
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
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje con patrones específicos para motos versátiles"""
        try:
            # Buscar en toda la página
            page_text = self.driver.page_source.lower()
            
            # Patrones específicos para motos versátiles (uso variado)
            km_patterns = [
                r'(\d+(?:\.\d{3})*)\s*km',
                r'(\d+(?:,\d{3})*)\s*km',
                r'(\d+(?:\.\d{3})*)\s*kilómetros',
                r'kilometraje[:\s]*(\d+(?:\.\d{3})*)',
                r'km[:\s]*(\d+(?:\.\d{3})*)',
                r'(\d+)\s*mil\s*km'
            ]
            
            for pattern in km_patterns:
                match = re.search(pattern, page_text)
                if match:
                    km_value = match.group(1)
                    # Convertir mil km a km
                    if 'mil' in match.group(0):
                        km_value = str(int(float(km_value.replace(',', '.')) * 1000))
                    return km_value
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo km: {e}")
            return "No especificado"
    
    def extract_vendedor(self) -> str:
        """Extraer vendedor con detección de comerciales específicos"""
        selectors = [
            "[data-testid='seller-name']",
            ".seller-name",
            ".user-info .name",
            ".seller-info .name",
            ".profile-name",
            ".user-name"
        ]
        
        vendedor = self._extract_text_by_selectors(selectors, "Particular")
        
        # Detectar si es comercial (común en motos populares como MT-07)
        commercial_keywords = ['concesionario', 'motor', 'moto', 'yamaha', 'taller', 'naked', 'mt']
        if any(word in vendedor.lower() for word in commercial_keywords):
            return f" {vendedor}"
        
        return vendedor

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_mt07_scraper():
    """Función principal para ejecutar el scraper de MT-07"""
    try:
        scraper = ScraperMT07()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f" Scraping MT-07 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print(" No se encontraron motos MT-07")
            return df_results
            
    except Exception as e:
        print(f" Error en scraper MT-07: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_mt07_scraper()
    if results is not None and not results.empty:
        print(f"\n Primeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")