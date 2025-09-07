"""
================================================================================
                    SCRAPER KYMCO AGILITY125 - WALLAPOP MOTOS SCRAPER                    
================================================================================

Scraper específico para Kymco Agility125
Implementa detección ultra precisa del modelo de scooter económico

Características:
- Detección específica de Kymco Agility125 y variantes
- Filtrado optimizado para scooters económicos
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
        
        self.logger.info(f" Scraper Agility125 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Kymco Agility125"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # URLs principales más efectivas para Agility125
        base_queries = [
            "kymco%20agility125",
            "kymco%20agility%20125", 
            "agility125",
            "agility%20125",
            "kymco%20agility",
            "scooter%20kymco%20agility",
            "kymco%20agility125%202020",
            "kymco%20agility125%202021",
            "kymco%20agility125%202022",
            "kymco%20agility125%202023",
            "kymco%20agility125%202024"
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
            query = "kymco%20agility125"
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&filters_source=search_box&latitude=40.4168&longitude=-3.7038&distance=100000")
        
        # URLs específicas para scooters económicos
        economic_queries = [
            "scooter%20125%20barato",
            "scooter%20economico%20125",
            "kymco%20125"
        ]
        
        for query in economic_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
        
        # Eliminar duplicados manteniendo orden
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"🔍 {len(unique_urls)} URLs generadas para Agility125")
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
                self.logger.debug(" No es Kymco")
                return False
            
            # PASO 2: Verificar modelo Agility125 específico
            agility125_found = self._is_agility125_model(combined_text)
            if not agility125_found:
                self.logger.debug(" No es Agility125")
                return False
            
            # PASO 3: Excluir otros modelos Agility o Kymco
            if self._is_excluded_model(combined_text):
                self.logger.debug(" Es otro modelo Kymco excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug(" Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug(" Año fuera de rango")
                return False
            
            self.logger.debug(f" Agility125 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f" Error validando moto: {e}")
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
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje con patrones específicos para scooters económicos"""
        try:
            # Buscar en toda la página
            page_text = self.driver.page_source.lower()
            
            # Patrones específicos para scooters (pueden tener más km por uso urbano)
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
        
        # Detectar si es comercial (menos común en scooters económicos)
        commercial_keywords = ['concesionario', 'motor', 'moto', 'kymco', 'taller', 'scooter']
        if any(word in vendedor.lower() for word in commercial_keywords):
            return f" {vendedor}"
        
        return vendedor

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_agility125_scraper():
    """Función principal para ejecutar el scraper de Agility125"""
    try:
        scraper = ScraperAGILITY125()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f" Scraping Agility125 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print(" No se encontraron motos Agility125")
            return df_results
            
    except Exception as e:
        print(f" Error en scraper Agility125: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_agility125_scraper()
    if results is not None and not results.empty:
        print(f"\n Primeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")