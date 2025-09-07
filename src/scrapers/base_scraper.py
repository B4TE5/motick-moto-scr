"""
================================================================================
                    BASE SCRAPER - WALLAPOP MOTOS SCRAPER                    
================================================================================

Clase base para todos los scrapers de modelos específicos
Contiene funcionalidad común para extracción de datos de Wallapop

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

import time
import re
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from typing import Dict, List, Optional, Tuple
from abc import ABC, abstractmethod

from config import get_modelo_config, SELENIUM_CONFIG, is_github_actions

class BaseScraper(ABC):
    """Clase base para scrapers de modelos específicos de motos"""
    
    def __init__(self, modelo_key: str):
        """
        Inicializar scraper base
        
        Args:
            modelo_key: Clave del modelo (ej: 'cb125r')
        """
        self.modelo_key = modelo_key
        self.modelo_config = get_modelo_config(modelo_key)
        self.selenium_config = SELENIUM_CONFIG
        self.logger = logging.getLogger(__name__)
        
        self.driver = None
        self.results = []
        self.processed_urls = set()
        
        self.logger.info(f" Scraper iniciado para {self.modelo_config['nombre']}")
    
    def setup_driver(self) -> webdriver.Chrome:
        """Configurar y retornar driver de Chrome optimizado"""
        options = Options()
        
        # Configuración básica
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-backgrounding-occluded-windows")
        
        # User agent
        options.add_argument(f"--user-agent={self.selenium_config['user_agent']}")
        
        # Configuración para GitHub Actions
        if is_github_actions():
            options.add_argument("--headless")
            options.add_argument("--disable-logging")
            options.add_argument("--log-level=3")
            options.add_argument("--silent")
        
        # Configuraciones de rendimiento
        prefs = {
            "profile.default_content_setting_values": {
                "images": 2,
                "plugins": 2,
                "popups": 2,
                "geolocation": 2,
                "notifications": 2,
                "media_stream": 2,
            },
            "profile.managed_default_content_settings": {
                "images": 2
            }
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            driver = webdriver.Chrome(options=options)
            
            # Configurar timeouts
            driver.implicitly_wait(self.selenium_config['implicit_wait'])
            driver.set_page_load_timeout(self.selenium_config['page_load_timeout'])
            driver.set_script_timeout(self.selenium_config['script_timeout'])
            
            if not is_github_actions():
                driver.maximize_window()
            
            self.logger.info(" Driver configurado correctamente")
            return driver
            
        except Exception as e:
            self.logger.error(f" Error configurando driver: {e}")
            raise
    
    def scrape_model(self) -> pd.DataFrame:
        """
        Método principal para hacer scraping de un modelo
        
        Returns:
            DataFrame con los resultados
        """
        try:
            self.logger.info(f" Iniciando scraping de {self.modelo_config['nombre']}")
            
            # Configurar driver
            self.driver = self.setup_driver()
            
            # Obtener URLs de búsqueda específicas del modelo
            search_urls = self.get_search_urls()
            self.logger.info(f" {len(search_urls)} URLs de búsqueda generadas")
            
            # Procesar cada URL
            for i, url in enumerate(search_urls, 1):
                self.logger.info(f" Procesando URL {i}/{len(search_urls)}: {url[:80]}...")
                
                try:
                    self.process_search_url(url)
                    time.sleep(2)  # Pausa entre URLs
                except Exception as e:
                    self.logger.warning(f" Error procesando URL {i}: {e}")
                    continue
            
            # Convertir resultados a DataFrame
            if self.results:
                df = pd.DataFrame(self.results)
                self.logger.info(f" Scraping completado: {len(df)} motos encontradas")
                return df
            else:
                self.logger.warning(" No se encontraron resultados")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f" Error durante scraping: {e}")
            return pd.DataFrame()
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info(" Driver cerrado correctamente")
                except:
                    pass
    
    def process_search_url(self, url: str):
        """Procesar una URL de búsqueda específica"""
        try:
            # Cargar página
            self.driver.get(url)
            time.sleep(3)
            
            # Obtener enlaces de anuncios
            enlaces = self.get_anuncio_links()
            
            if not enlaces:
                self.logger.warning(f" No se encontraron enlaces en esta URL")
                return
            
            self.logger.info(f" {len(enlaces)} enlaces encontrados")
            
            # Procesar cada anuncio
            for i, enlace in enumerate(enlaces, 1):
                if enlace in self.processed_urls:
                    continue
                
                try:
                    self.logger.debug(f" Procesando anuncio {i}/{len(enlaces)}")
                    moto_data = self.extract_anuncio_data(enlace)
                    
                    if moto_data and self.validate_moto_data(moto_data):
                        self.results.append(moto_data)
                        self.processed_urls.add(enlace)
                        self.logger.debug(f" Moto válida extraída: {moto_data.get('Título', 'Sin título')}")
                    else:
                        self.logger.debug(f" Moto no válida o datos incompletos")
                    
                    time.sleep(1)  # Pausa entre anuncios
                    
                except Exception as e:
                    self.logger.warning(f" Error extrayendo anuncio {i}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f" Error procesando URL de búsqueda: {e}")
    
    def get_anuncio_links(self) -> List[str]:
        """Obtener enlaces de anuncios de la página actual"""
        enlaces = []
        
        try:
            # Hacer scroll para cargar más contenido
            self.scroll_to_load_content()
            
            # Buscar enlaces de anuncios
            link_selectors = [
                "a[href*='/item/']",
                "a[href*='/app/']",
                ".card-product-info a",
                ".item-card a",
                "[data-testid='item-card'] a"
            ]
            
            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and '/item/' in href and href not in enlaces:
                            enlaces.append(href)
                except:
                    continue
            
            # Filtrar enlaces válidos
            enlaces_validos = []
            for enlace in enlaces:
                if self.is_valid_anuncio_link(enlace):
                    enlaces_validos.append(enlace)
            
            return enlaces_validos
            
        except Exception as e:
            self.logger.error(f" Error obteniendo enlaces: {e}")
            return []
    
    def scroll_to_load_content(self):
        """Hacer scroll para cargar contenido dinámico"""
        try:
            # Scroll gradual para cargar contenido
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            for attempt in range(self.selenium_config['max_scroll_attempts']):
                # Scroll hacia abajo
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(self.selenium_config['scroll_pause'])
                
                # Verificar si se cargó más contenido
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Volver arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
        except Exception as e:
            self.logger.warning(f" Error durante scroll: {e}")
    
    def extract_anuncio_data(self, url: str) -> Optional[Dict]:
        """
        Extraer datos de un anuncio específico
        
        Args:
            url: URL del anuncio
            
        Returns:
            Diccionario con datos del anuncio o None si falla
        """
        try:
            # Cargar página del anuncio
            self.driver.get(url)
            time.sleep(2)
            
            # Extraer datos usando métodos específicos
            data = {
                'URL': url,
                'Título': self.extract_titulo(),
                'Precio': self.extract_precio(),
                'Kilometraje': self.extract_kilometraje(),
                'Año': self.extract_año(),
                'Vendedor': self.extract_vendedor(),
                'Ubicación': self.extract_ubicacion(),
                'Fecha_Publicacion': self.extract_fecha_publicacion(),
                'Descripcion': self.extract_descripcion(),
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            self.logger.warning(f" Error extrayendo datos de {url}: {e}")
            return None
    
    # Métodos abstractos que deben implementar las subclases
    @abstractmethod
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda específicas del modelo"""
        pass
    
    @abstractmethod
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos de la moto corresponden al modelo específico"""
        pass
    
    # Métodos de extracción que pueden ser sobrescritos
    def extract_titulo(self) -> str:
        """Extraer título del anuncio"""
        selectors = [
            "h1",
            ".item-detail-title",
            "[data-testid='item-title']",
            ".title",
            "h2"
        ]
        
        return self._extract_text_by_selectors(selectors, "Sin título")
    
    def extract_precio(self) -> str:
        """Extraer precio del anuncio"""
        selectors = [
            ".item-price",
            "[data-testid='item-price']",
            ".price",
            ".item-detail-price",
            "span[class*='price']"
        ]
        
        return self._extract_text_by_selectors(selectors, "No especificado")
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje del anuncio"""
        try:
            # Buscar en texto de la página
            page_text = self.driver.page_source.lower()
            
            # Patrones para kilometraje
            km_patterns = [
                r'(\d+(?:\.\d{3})*)\s*k?m',
                r'(\d+(?:,\d{3})*)\s*k?m',
                r'kilometraje[:\s]*(\d+(?:\.\d{3})*)',
                r'kilómetros[:\s]*(\d+(?:\.\d{3})*)'
            ]
            
            for pattern in km_patterns:
                match = re.search(pattern, page_text)
                if match:
                    return match.group(1)
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def extract_año(self) -> str:
        """Extraer año del anuncio"""
        try:
            # Buscar año en título y texto
            titulo = self.extract_titulo()
            page_text = self.driver.page_source
            combined_text = f"{titulo} {page_text}"
            
            # Buscar año de 4 dígitos
            year_match = re.search(r'(20[0-9]{2}|19[8-9][0-9])', combined_text)
            if year_match:
                year = int(year_match.group(1))
                current_year = datetime.now().year
                if 1990 <= year <= current_year + 1:
                    return str(year)
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def extract_vendedor(self) -> str:
        """Extraer información del vendedor"""
        selectors = [
            "[data-testid='seller-name']",
            ".seller-name",
            ".user-name",
            ".seller-info .name",
            ".profile-name"
        ]
        
        return self._extract_text_by_selectors(selectors, "Particular")
    
    def extract_ubicacion(self) -> str:
        """Extraer ubicación del anuncio"""
        selectors = [
            "[data-testid='item-location']",
            ".item-location",
            ".location",
            ".item-detail-location"
        ]
        
        return self._extract_text_by_selectors(selectors, "No especificado")
    
    def extract_fecha_publicacion(self) -> str:
        """Extraer fecha de publicación"""
        try:
            page_text = self.driver.page_source.lower()
            
            # Patrones de fecha
            date_patterns = [
                r'hace\s+(\d+)\s+día',
                r'hace\s+(\d+)\s+hora',
                r'ayer',
                r'hoy'
            ]
            
            for pattern in date_patterns:
                if re.search(pattern, page_text):
                    return "Reciente"
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def extract_descripcion(self) -> str:
        """Extraer descripción del anuncio"""
        selectors = [
            "[data-testid='item-description']",
            ".item-description",
            ".description",
            ".item-detail-description"
        ]
        
        desc = self._extract_text_by_selectors(selectors, "")
        return desc[:200] + "..." if len(desc) > 200 else desc
    
    def _extract_text_by_selectors(self, selectors: List[str], default: str = "") -> str:
        """Extraer texto usando múltiples selectores"""
        for selector in selectors:
            try:
                element = WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                text = element.text.strip()
                if text:
                    return text
            except:
                continue
        return default
    
    def is_valid_anuncio_link(self, url: str) -> bool:
        """Validar si un enlace es válido para procesar"""
        if not url or '/item/' not in url:
            return False
        
        # Filtrar URLs no deseadas
        excluded_patterns = [
            'promoted',
            'destacado', 
            'premium',
            'banner'
        ]
        
        for pattern in excluded_patterns:
            if pattern in url.lower():
                return False
        
        return True

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def test_base_scraper():
    """Función de prueba para la funcionalidad base"""
    print(" Probando funcionalidad base del scraper...")
    
    try:
        # Crear implementación mínima para prueba
        class TestScraper(BaseScraper):
            def get_search_urls(self):
                return ["https://es.wallapop.com/app/search?keywords=honda%20cb125r"]
            
            def validate_moto_data(self, moto_data):
                return True
        
        scraper = TestScraper('cb125r')
        driver = scraper.setup_driver()
        
        print(" Driver configurado correctamente")
        driver.quit()
        print(" Driver cerrado correctamente")
        
        return True
        
    except Exception as e:
        print(f" Error en prueba: {e}")
        return False

if __name__ == "__main__":
    # Prueba de la clase base
    test_base_scraper()