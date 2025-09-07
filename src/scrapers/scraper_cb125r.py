"""
================================================================================
                    SCRAPER HONDA CB125R - WALLAPOP MOTOS SCRAPER                    
================================================================================

Scraper específico para Honda CB125R
Implementa detección ultra precisa del modelo y extracción optimizada

Características:
- Detección específica de Honda CB125R y variantes
- Filtrado en dos fases para máxima eficiencia
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

class ScraperCB125R(BaseScraper):
    """Scraper específico para Honda CB125R"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bcb[\-\s]*125[\-\s]*r\b',
            r'\bcb125r\b',
            r'\bcb\s*125\s*r\b',
            r'\bcb\s*125[\-\.\/]\s*r\b'
        ]
        
        self.exclude_patterns = [
            r'\bcb[\-\s]*125[\-\s]*f\b',  # CB125F
            r'\bcb[\-\s]*250[\-\s]*r\b',  # CB250R
            r'\bcb[\-\s]*500[\-\s]*r\b',  # CB500R
            r'\bcb[\-\s]*650[\-\s]*r\b',  # CB650R
            r'\bcbr[\-\s]*125\b',         # CBR125 (diferente modelo)
            r'\bcbr\b'                    # CBR en general
        ]
        
        self.logger.info(f" Scraper CB125R inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Honda CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # URLs principales más efectivas para CB125R
        base_queries = [
            "honda%20cb125r",
            "honda%20cb%20125%20r", 
            "cb125r",
            "cb%20125%20r",
            "honda%20cb125r%202020",
            "honda%20cb125r%202021",
            "honda%20cb125r%202022",
            "honda%20cb125r%202023",
            "honda%20cb125r%202024"
        ]
        
        # Generar URLs con diferentes ordenamientos
        for query in base_queries:
            # URL básica con filtro de precio
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
            
            # URL ordenada por más recientes
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest")
        
        # URLs específicas por regiones principales (mejor cobertura)
        main_regions = ["madrid", "barcelona", "valencia", "sevilla", "bilbao"]
        for region in main_regions[:3]:  # Solo 3 regiones principales
            query = "honda%20cb125r"
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&filters_source=search_box&latitude=40.4168&longitude=-3.7038&distance=100000")
        
        # Eliminar duplicados manteniendo orden
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f" {len(unique_urls)} URLs generadas para CB125R")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validar si los datos corresponden realmente a una Honda CB125R
        
        Args:
            moto_data: Diccionario con datos de la moto
            
        Returns:
            bool: True si es válida CB125R
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
            
            # PASO 2: Verificar modelo CB125R específico
            cb125r_found = self._is_cb125r_model(combined_text)
            if not cb125r_found:
                self.logger.debug(" No es CB125R")
                return False
            
            # PASO 3: Excluir otros modelos CB
            if self._is_excluded_model(combined_text):
                self.logger.debug(" Es otro modelo CB excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug(" Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug(" Año fuera de rango")
                return False
            
            self.logger.debug(f" CB125R válida: {titulo[:50]}")
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
    
    def _is_cb125r_model(self, text: str) -> bool:
        """Verificar si es modelo CB125R específico"""
        for pattern in self.model_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_excluded_model(self, text: str) -> bool:
        """Verificar si es un modelo excluido (CB125F, CB250R, etc.)"""
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
    
    def extract_titulo(self) -> str:
        """Extraer título específico para CB125R"""
        # Usar método base pero con selectores adicionales específicos
        selectors = [
            "h1[data-testid='item-title']",
            "h1.item-detail-title", 
            "h1",
            ".item-title",
            "[data-testid='item-title']",
            ".title-section h1",
            ".product-title"
        ]
        
        return self._extract_text_by_selectors(selectors, "Honda CB125R")
    
    def extract_precio(self) -> str:
        """Extraer precio específico para CB125R"""
        selectors = [
            "[data-testid='item-price']",
            ".item-price .price",
            ".item-detail-price",
            ".price-current",
            "span[class*='price']",
            ".seller-info .price"
        ]
        
        return self._extract_text_by_selectors(selectors, "No especificado")
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje con patrones específicos para motos"""
        try:
            # Buscar en toda la página
            page_text = self.driver.page_source.lower()
            
            # Patrones específicos para motos
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
    
    def extract_año(self) -> str:
        """Extraer año con búsqueda específica para CB125R"""
        try:
            # Buscar en título y descripción primero
            titulo = self.extract_titulo()
            
            # Buscar año en título (más preciso)
            for year in range(self.modelo_config['año_max'], self.modelo_config['año_min'] - 1, -1):
                if str(year) in titulo:
                    return str(year)
            
            # Buscar en página completa
            page_text = self.driver.page_source
            year_match = re.search(r'(20[1-2][0-9])', page_text)
            if year_match:
                year = int(year_match.group(1))
                if self.modelo_config['año_min'] <= year <= self.modelo_config['año_max']:
                    return str(year)
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo año: {e}")
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
        
        # Detectar si es comercial
        if any(word in vendedor.lower() for word in ['concesionario', 'motor', 'moto', 'honda', 'taller']):
            return f" {vendedor}"
        
        return vendedor
    
    def extract_ubicacion(self) -> str:
        """Extraer ubicación con formato específico"""
        selectors = [
            "[data-testid='item-location']",
            ".item-location",
            ".location-info",
            ".seller-location",
            ".item-detail-location"
        ]
        
        ubicacion = self._extract_text_by_selectors(selectors, "No especificado")
        
        # Limpiar ubicación
        if ubicacion and ubicacion != "No especificado":
            ubicacion = ubicacion.replace("en ", "").strip()
        
        return ubicacion

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_cb125r_scraper():
    """Función principal para ejecutar el scraper de CB125R"""
    try:
        scraper = ScraperCB125R()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f" Scraping CB125R completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print(" No se encontraron motos CB125R")
            return df_results
            
    except Exception as e:
        print(f" Error en scraper CB125R: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_cb125r_scraper()
    if results is not None and not results.empty:
        print(f"\n Primeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")