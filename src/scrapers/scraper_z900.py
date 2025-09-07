"""
================================================================================
                    SCRAPER KAWASAKI Z900 - WALLAPOP MOTOS SCRAPER                    
================================================================================

Scraper específico para Kawasaki Z900
Implementa detección ultra precisa del modelo naked de alta cilindrada

Características:
- Detección específica de Kawasaki Z900 y variantes (Z900, Z900RS, Z900E)
- Filtrado optimizado para motos naked de alta cilindrada
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
        
        self.logger.info(f" Scraper Z900 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Kawasaki Z900"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # URLs principales más efectivas para Z900
        base_queries = [
            "kawasaki%20z900",
            "kawasaki%20z%20900", 
            "z900",
            "z%20900",
            "kawasaki%20z900%20naked",
            "naked%20kawasaki%20z900",
            "kawasaki%20z900rs",
            "z900%20rs",
            "kawasaki%20z900%202017",
            "kawasaki%20z900%202018",
            "kawasaki%20z900%202019",
            "kawasaki%20z900%202020",
            "kawasaki%20z900%202021",
            "kawasaki%20z900%202022",
            "kawasaki%20z900%202023",
            "kawasaki%20z900%202024"
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
            query = "kawasaki%20z900"
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&filters_source=search_box&latitude=40.4168&longitude=-3.7038&distance=100000")
        
        # URLs específicas para motos naked premium
        naked_queries = [
            "naked%20kawasaki%20900",
            "kawasaki%20naked%20900",
            "moto%20naked%20premium",
            "kawasaki%20z%20serie"
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
        
        self.logger.info(f"🔍 {len(unique_urls)} URLs generadas para Z900")
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
                self.logger.debug(" No es Kawasaki")
                return False
            
            # PASO 2: Verificar modelo Z900 específico
            z900_found = self._is_z900_model(combined_text)
            if not z900_found:
                self.logger.debug(" No es Z900")
                return False
            
            # PASO 3: Excluir otros modelos Z de Kawasaki
            if self._is_excluded_model(combined_text):
                self.logger.debug(" Es otro modelo Z de Kawasaki excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug(" Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug(" Año fuera de rango")
                return False
            
            self.logger.debug(f" Z900 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f" Error validando moto: {e}")
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
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje con patrones específicos para motos naked premium"""
        try:
            # Buscar en toda la página
            page_text = self.driver.page_source.lower()
            
            # Patrones específicos para motos premium (menos km generalmente)
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
        
        # Detectar si es comercial (muy común en motos de alta cilindrada)
        commercial_keywords = ['concesionario', 'motor', 'moto', 'kawasaki', 'taller', 'naked', 'racing', 'premium']
        if any(word in vendedor.lower() for word in commercial_keywords):
            return f"🏢 {vendedor}"
        
        return vendedor

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_z900_scraper():
    """Función principal para ejecutar el scraper de Z900"""
    try:
        scraper = ScraperZ900()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f" Scraping Z900 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print(" No se encontraron motos Z900")
            return df_results
            
    except Exception as e:
        print(f" Error en scraper Z900: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_z900_scraper()
    if results is not None and not results.empty:
        print(f"\n Primeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")