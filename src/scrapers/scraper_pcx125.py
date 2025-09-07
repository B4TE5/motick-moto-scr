"""
================================================================================
                    SCRAPER HONDA PCX125 - WALLAPOP MOTOS SCRAPER                    
================================================================================

Scraper específico para Honda PCX125
Implementa detección ultra precisa del modelo de scooter urbano

Características:
- Detección específica de Honda PCX125 y variantes
- Filtrado optimizado para scooters urbanos
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
        
        self.logger.info(f" Scraper PCX125 inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Honda PCX125"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # URLs principales más efectivas para PCX125
        base_queries = [
            "honda%20pcx125",
            "honda%20pcx%20125", 
            "pcx125",
            "pcx%20125",
            "honda%20pcx",
            "scooter%20honda%20pcx125",
            "honda%20pcx125%202020",
            "honda%20pcx125%202021",
            "honda%20pcx125%202022",
            "honda%20pcx125%202023",
            "honda%20pcx125%202024"
        ]
        
        # Generar URLs con diferentes ordenamientos
        for query in base_queries:
            # URL básica con filtro de precio
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
            
            # URL ordenada por más recientes
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest")
        
        # URLs específicas por regiones principales (scooters urbanos se concentran en ciudades)
        main_regions = ["madrid", "barcelona", "valencia", "sevilla"]
        for region in main_regions[:3]:  # Solo 3 regiones principales
            query = "honda%20pcx125"
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&filters_source=search_box&latitude=40.4168&longitude=-3.7038&distance=100000")
        
        # URLs específicas para scooters
        scooter_queries = [
            "scooter%20honda%20125",
            "honda%20scooter%20125"
        ]
        
        for query in scooter_queries:
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
        
        # Eliminar duplicados manteniendo orden
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f" {len(unique_urls)} URLs generadas para PCX125")
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
                self.logger.debug(" No es Honda")
                return False
            
            # PASO 2: Verificar modelo PCX125 específico
            pcx125_found = self._is_pcx125_model(combined_text)
            if not pcx125_found:
                self.logger.debug(" No es PCX125")
                return False
            
            # PASO 3: Excluir otros modelos PCX o scooters
            if self._is_excluded_model(combined_text):
                self.logger.debug(" Es otro modelo PCX o scooter excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug(" Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug(" Año fuera de rango")
                return False
            
            self.logger.debug(f" PCX125 válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f" Error validando moto: {e}")
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
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje con patrones específicos para scooters"""
        try:
            # Buscar en toda la página
            page_text = self.driver.page_source.lower()
            
            # Patrones específicos para scooters (suelen tener menos km)
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
        """Extraer vendedor con detección de comerciales específicos para scooters"""
        selectors = [
            "[data-testid='seller-name']",
            ".seller-name",
            ".user-info .name",
            ".seller-info .name",
            ".profile-name",
            ".user-name"
        ]
        
        vendedor = self._extract_text_by_selectors(selectors, "Particular")
        
        # Detectar si es comercial (común en scooters)
        commercial_keywords = ['concesionario', 'motor', 'moto', 'honda', 'taller', 'scooter', 'yamaha']
        if any(word in vendedor.lower() for word in commercial_keywords):
            return f" {vendedor}"
        
        return vendedor

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_pcx125_scraper():
    """Función principal para ejecutar el scraper de PCX125"""
    try:
        scraper = ScraperPCX125()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f" Scraping PCX125 completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print(" No se encontraron motos PCX125")
            return df_results
            
    except Exception as e:
        print(f" Error en scraper PCX125: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_pcx125_scraper()
    if results is not None and not results.empty:
        print(f"\n Primeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")