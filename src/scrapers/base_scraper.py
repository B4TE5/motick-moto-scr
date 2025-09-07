"""
================================================================================
                    BASE SCRAPER ACTUALIZADO - WALLAPOP MOTOS SCRAPER                    
================================================================================

Clase base actualizada con selectores reales de Wallapop y extracción robusta
Basado en el HTML real y la función de extracción que funcionaba en local

Autor: Carlos Peraza
Versión: 1.1 - Actualizada para Wallapop actual
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
    """Clase base actualizada para scrapers de modelos específicos de motos"""
    
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
        """Configurar y retornar driver de Chrome optimizado para GitHub Actions"""
        options = Options()
        
        # Configuración básica optimizada
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
        
        # User agent actualizado
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
        
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
            
            # Configurar timeouts más cortos para evitar colgadas
            driver.implicitly_wait(0.5)  # Reducido
            driver.set_page_load_timeout(10)  # Reducido de 15 a 10
            driver.set_script_timeout(8)  # Reducido de 10 a 8
            
            if not is_github_actions():
                driver.maximize_window()
            
            self.logger.info(" Driver configurado correctamente")
            return driver
            
        except Exception as e:
            self.logger.error(f" Error configurando driver: {e}")
            raise
    
    def scrape_model(self) -> pd.DataFrame:
        """
        Método principal para hacer scraping de un modelo con mejor manejo de errores
        
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
            
            # Procesar cada URL con límite de tiempo
            start_time = time.time()
            max_total_time = 1800 if is_github_actions() else 3600  # 30 min en GH Actions, 1h local
            
            for i, url in enumerate(search_urls, 1):
                # Verificar tiempo total
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    self.logger.warning(f" Tiempo límite alcanzado ({max_total_time/60:.1f} min)")
                    break
                
                self.logger.info(f" Procesando URL {i}/{len(search_urls)}: {url[:80]}...")
                
                try:
                    self.process_search_url_with_timeout(url)
                    time.sleep(1)  # Pausa reducida entre URLs
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
            self.logger.error(f"❌ Error durante scraping: {e}")
            return pd.DataFrame()
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info(" Driver cerrado correctamente")
                except:
                    pass
    
    def process_search_url_with_timeout(self, url: str):
        """Procesar una URL de búsqueda con timeout"""
        max_url_time = 600 if is_github_actions() else 900  # 10 min en GH Actions
        start_url_time = time.time()
        
        try:
            # Cargar página con timeout
            self.driver.get(url)
            time.sleep(2)
            
            # Obtener enlaces de anuncios
            enlaces = self.get_anuncio_links()
            
            if not enlaces:
                self.logger.warning(f" No se encontraron enlaces en esta URL")
                return
            
            self.logger.info(f"🔗 {len(enlaces)} enlaces encontrados")
            
            # Procesar cada anuncio con límite
            max_anuncios = 10 if is_github_actions() else len(enlaces)  # Límite en GH Actions
            anuncios_procesados = 0
            
            for i, enlace in enumerate(enlaces[:max_anuncios], 1):
                # Verificar timeout de URL
                elapsed_url_time = time.time() - start_url_time
                if elapsed_url_time > max_url_time:
                    self.logger.warning(f" Timeout de URL alcanzado")
                    break
                
                if enlace in self.processed_urls:
                    continue
                
                try:
                    self.logger.debug(f" Procesando anuncio {i}/{min(max_anuncios, len(enlaces))}")
                    moto_data = self.extract_anuncio_data_robust(enlace)
                    
                    if moto_data and self.validate_moto_data(moto_data):
                        self.results.append(moto_data)
                        self.processed_urls.add(enlace)
                        anuncios_procesados += 1
                        self.logger.debug(f" Moto válida: {moto_data.get('Título', 'Sin título')[:30]}")
                    else:
                        self.logger.debug(f" Moto no válida o datos incompletos")
                    
                    time.sleep(0.5)  # Pausa muy corta entre anuncios
                    
                except Exception as e:
                    self.logger.warning(f" Error extrayendo anuncio {i}: {e}")
                    continue
            
            self.logger.info(f" URL procesada: {anuncios_procesados} motos válidas de {len(enlaces)} enlaces")
                    
        except Exception as e:
            self.logger.error(f" Error procesando URL de búsqueda: {e}")
    
    def get_anuncio_links(self) -> List[str]:
        """Obtener enlaces de anuncios con selectores actualizados"""
        enlaces = []
        
        try:
            # Hacer scroll para cargar contenido
            self.scroll_to_load_content()
            
            # Selectores actualizados basados en la estructura real de Wallapop
            link_selectors = [
                "a[href*='/item/']",
                ".card-product-info a",
                ".item-card a",
                "[data-testid='item-card'] a",
                "a[data-testid*='item']"
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
        """Hacer scroll optimizado para cargar contenido dinámico"""
        try:
            # Scroll más rápido y limitado
            max_scrolls = 2 if is_github_actions() else 3
            
            for attempt in range(max_scrolls):
                current_height = self.driver.execute_script("return document.body.scrollHeight")
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Reducido
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == current_height:
                    break
            
            # Volver arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
            
        except Exception as e:
            self.logger.warning(f" Error durante scroll: {e}")
    
    def extract_anuncio_data_robust(self, url: str) -> Optional[Dict]:
        """
        Extraer datos con selectores actualizados de Wallapop
        
        Args:
            url: URL del anuncio
            
        Returns:
            Diccionario con datos del anuncio o None si falla
        """
        try:
            # Cargar página del anuncio con timeout más corto
            self.driver.get(url)
            time.sleep(1.5)  # Reducido
            
            # Extraer datos usando selectores actualizados
            data = {
                'URL': url,
                'Título': self.extract_titulo_wallapop(),
                'Precio': self.extract_precio_wallapop(),
                'Kilometraje': self.extract_kilometraje_wallapop(),
                'Año': self.extract_año_wallapop(),
                'Vendedor': self.extract_vendedor_wallapop(),
                'Ubicación': self.extract_ubicacion_wallapop(),
                'Fecha_Publicacion': self.extract_fecha_publicacion_wallapop(),
                'Descripcion': self.extract_descripcion_wallapop(),
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            self.logger.warning(f" Error extrayendo datos de {url}: {e}")
            return None
    
    # Métodos de extracción actualizados con selectores reales de Wallapop
    
    def extract_titulo_wallapop(self) -> str:
        """Extraer título con selector real de Wallapop"""
        selectors = [
            "h1.item-detail_ItemDetailTwoColumns__title__VtWrR",
            "h1[class*='ItemDetailTwoColumns__title']",
            "h1[class*='title']",
            "h1"
        ]
        
        return self._extract_text_by_selectors(selectors, "Sin título")
    
    def extract_precio_wallapop(self) -> str:
        """Extraer precio con selectores reales de Wallapop"""
        selectors = [
            "span.item-detail-price_ItemDetailPrice--standard__fMa16",
            "span[class*='ItemDetailPrice--standard']",
            "span[class*='ItemDetailPrice']",
            "[class*='price'] span",
            ".price"
        ]
        
        precio_raw = self._extract_text_by_selectors(selectors, "No especificado")
        
        # Limpiar precio
        if precio_raw and precio_raw != "No especificado":
            # Remover &nbsp; y espacios extra
            precio_clean = precio_raw.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
            return precio_clean
        
        return "No especificado"
    
    def extract_descripcion_wallapop(self) -> str:
        """Extraer descripción completa para análisis de año y km"""
        selectors = [
            "section.item-detail_ItemDetailTwoColumns__description__0DKb0",
            "section[class*='description']",
            "[class*='description']",
            ".description"
        ]
        
        descripcion = self._extract_text_by_selectors(selectors, "")
        return descripcion[:500] if descripcion else ""  # Limitar tamaño
    
    def extract_año_wallapop(self) -> str:
        """Extraer año usando la función robusta"""
        try:
            # Obtener descripción completa
            descripcion = self.extract_descripcion_wallapop()
            titulo = self.extract_titulo_wallapop()
            
            # Usar la función robusta que funcionaba en local
            año, _ = self.extract_year_and_km_universal(f"{titulo} {descripcion}")
            return año
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo año: {e}")
            return "No especificado"
    
    def extract_kilometraje_wallapop(self) -> str:
        """Extraer kilometraje usando la función robusta"""
        try:
            # Obtener descripción completa
            descripcion = self.extract_descripcion_wallapop()
            titulo = self.extract_titulo_wallapop()
            
            # Usar la función robusta que funcionaba en local
            _, km = self.extract_year_and_km_universal(f"{titulo} {descripcion}")
            return km if km else "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo km: {e}")
            return "No especificado"
    
    def extract_year_and_km_universal(self, text: str) -> Tuple[str, str]:
        """
        Función robusta de extracción basada en la que funcionaba en local
        Adaptada para CB125R y otros modelos
        """
        if not text:
            return "No especificado", "No especificado"
        
        text_normalized = text.lower().replace('\n', ' ').replace('\t', ' ')
        text_normalized = re.sub(r'\s+', ' ', text_normalized)
        
        # ===== EXTRACCIÓN DE AÑO =====
        year = "No especificado"
        all_year_candidates = []
        
        # Patrones de año optimizados
        year_patterns = [
            # Patrones específicos con alta prioridad
            (r'año\s*[:\-]\s*(201[3-9]|202[0-4])', 22, "año: directo"),
            (r'año\s+(201[3-9]|202[0-4])', 20, "año XXXX"),
            (r'del\s+año\s+(201[3-9]|202[0-4])', 20, "del año XXXX"),
            (r'(?:modelo|model)\s+(?:del\s+)?(?:año\s+)?(201[3-9]|202[0-4])', 20, "modelo del año"),
            (r'matriculad[ao]\s+(?:en\s+(?:el\s+año\s+)?)?(?:en\s+)?(201[3-9]|202[0-4])', 18, "matriculada en XXXX"),
            (r'(?:honda|yamaha|kawasaki|kymco|cb|mt|z|pcx|agility)\s+(?:del\s+año\s+|año\s+|de\s+)?(201[3-9]|202[0-4])', 18, "marca/modelo del año"),
            (r'del\s+(201[3-9]|202[0-4])(?!\s*(?:€|euros|km|kms|klm))(?:\s|$|\.|\,)', 14, "del XXXX"),
            (r'\b(201[3-9]|202[0-4])\b(?!\s*(?:€|euros|km|kms|klm|cc|cv|hp|precio))', 8, "año standalone"),
        ]
        
        # Patrones a excluir
        exclude_patterns = [
            r'(?:itv|itvs)\s+(?:hasta|vigente|válida|pasada|en|del|año|de)\s+(201[3-9]|202[0-4])',
            r'revisión\s+(?:en|del|año|hasta|de)\s+(201[3-9]|202[0-4])',
            r'(201[3-9]|202[0-4])€',
            r'€(201[3-9]|202[0-4])',
            r'(201[3-9]|202[0-4])cc',
        ]
        
        # Buscar candidatos de año
        for pattern, score, description in year_patterns:
            matches = re.finditer(pattern, text_normalized)
            for match in matches:
                found_year = match.group(1)
                
                try:
                    year_value = int(found_year)
                    if 2013 <= year_value <= 2024:  # Rango válido para motos
                        # Verificar que no está excluido
                        is_excluded = False
                        for exclude_pattern in exclude_patterns:
                            if re.search(exclude_pattern, match.group(0)):
                                is_excluded = True
                                break
                        
                        if not is_excluded:
                            all_year_candidates.append({
                                'year': found_year,
                                'score': score
                            })
                except ValueError:
                    continue
        
        # Seleccionar el año más conservador (menor)
        if all_year_candidates:
            year = min(candidate['year'] for candidate in all_year_candidates)
        
        # ===== EXTRACCIÓN DE KILÓMETROS =====
        km = "No especificado"
        all_km_candidates = []
        
        km_patterns = [
            # Patrones prioritarios específicos
            r'km\s*[:\-]\s*(\d{1,3})\.(\d{3})',  # km: 10.500
            r'kilómetros?\s*[:\-]\s*(\d{1,3})\.(\d{3})',  # kilómetros: 10.500
            r'km\s*[:\-]\s*(\d+)',  # km: 0 (permitir 0)
            r'(\d{1,3})\.(\d{3})\s*km',  # 10.500 km
            r'(\d+)\s*km',  # 0 km (permitir 0)
            r'(?:solo|tiene|lleva|marca)\s+(\d{1,3})\.(\d{3})',  # solo 10.500
            r'(?:solo|tiene|lleva|marca)\s+(\d+)',  # solo 0
            r'(\d+)\s*mil\s*km',  # 10 mil km
        ]
        
        for pattern in km_patterns:
            matches = re.finditer(pattern, text_normalized)
            for match in matches:
                groups = match.groups()
                
                try:
                    if len(groups) == 1:
                        km_value = int(groups[0])
                        if 'mil' in pattern:
                            km_value = km_value * 1000
                    elif len(groups) == 2:
                        if 'mil' in pattern:
                            km_value = int(groups[0]) * 1000 + int(groups[1])
                        else:
                            # Formato español (punto como separador miles)
                            km_value = int(groups[0] + groups[1])
                    
                    # Permitir km = 0 y hasta 200,000 km
                    if 0 <= km_value <= 200000:
                        all_km_candidates.append({
                            'value': km_value,
                            'context': match.group(0)
                        })
                        
                except (ValueError, TypeError):
                    continue
        
        # Seleccionar el KM más alto (más conservador)
        if all_km_candidates:
            # Filtrar contextos obviamente incorrectos
            exclude_km_keywords = ['rueda', 'neumático', 'revisión', 'service', 'velocidad', 'hora']
            valid_candidates = []
            
            for c in all_km_candidates:
                text_context = c['context'].lower()
                if not any(keyword in text_context for keyword in exclude_km_keywords):
                    valid_candidates.append(c)
            
            if valid_candidates:
                km_value = max(c['value'] for c in valid_candidates)
                if km_value == 0:
                    km = "0 km"
                else:
                    km = f"{km_value:,} km".replace(',', '.')
        
        return year, km
    
    def extract_vendedor_wallapop(self) -> str:
        """Extraer vendedor con selector real de Wallapop"""
        selectors = [
            "h3.text-truncate.mb-0.mt-md-0.me-2",
            "h3[class*='text-truncate']",
            "[class*='seller'] h3",
            ".seller-name",
            "h3"
        ]
        
        return self._extract_text_by_selectors(selectors, "Particular")
    
    def extract_ubicacion_wallapop(self) -> str:
        """Extraer ubicación con selector real de Wallapop"""
        selectors = [
            "a.item-detail-location_ItemDetailLocation__link__OmVsa",
            "a[class*='ItemDetailLocation__link']",
            "[class*='location'] a",
            ".location a"
        ]
        
        ubicacion = self._extract_text_by_selectors(selectors, "No especificado")
        
        # Limpiar ubicación
        if ubicacion and ubicacion != "No especificado":
            ubicacion = ubicacion.replace("en ", "").strip()
        
        return ubicacion
    
    def extract_fecha_publicacion_wallapop(self) -> str:
        """Extraer fecha con selector real de Wallapop"""
        selectors = [
            "span.item-detail-stats_ItemDetailStats__description__015I3",
            "span[class*='ItemDetailStats__description']",
            "[class*='stats'] span",
            ".stats span"
        ]
        
        return self._extract_text_by_selectors(selectors, "No especificado")
    
    def _extract_text_by_selectors(self, selectors: List[str], default: str = "") -> str:
        """Extraer texto usando múltiples selectores con timeout corto"""
        for selector in selectors:
            try:
                element = WebDriverWait(self.driver, 1).until(  # Timeout muy corto
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
    
    # Métodos abstractos que deben implementar las subclases
    @abstractmethod
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda específicas del modelo"""
        pass
    
    @abstractmethod
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos de la moto corresponden al modelo específico"""
        pass

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def test_base_scraper_actualizado():
    """Función de prueba para la funcionalidad base actualizada"""
    print(" Probando funcionalidad base actualizada del scraper...")
    
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
    # Prueba de la clase base actualizada
    test_base_scraper_actualizado()
