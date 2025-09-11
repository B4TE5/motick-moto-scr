"""
================================================================================
                            BASE SCRAPER CORREGIDO                      
================================================================================

Autor: Carlos Peraza
Version: 4.0 
Fecha: Septiembre 2025

CORRECCIONES APLICADAS:
1. Consistencia en nombres de campos (Titulo sin tilde)
2. Extractores mejorados y más robustos
3. Debug extenso para identificar problemas
4. Manejo de errores mejorado

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
from selenium.webdriver.common.action_chains import ActionChains
from typing import Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from tqdm import tqdm

from config import get_modelo_config, SELENIUM_CONFIG, is_github_actions

class BaseScraper(ABC):
    """Clase base CORREGIDA para scrapers con debug extenso"""
    
    def __init__(self, modelo_key: str):
        """Inicializar scraper base"""
        self.modelo_key = modelo_key
        self.modelo_config = get_modelo_config(modelo_key)
        self.selenium_config = SELENIUM_CONFIG
        self.logger = logging.getLogger(__name__)
        
        self.driver = None
        self.results = []
        self.processed_urls = set()
        
        # CONTADORES PARA DEBUG
        self.debug_counters = {
            'urls_procesadas': 0,
            'enlaces_encontrados': 0,
            'anuncios_procesados': 0,
            'datos_extraidos': 0,
            'validaciones_exitosas': 0,
            'errores_extraccion': 0
        }
        
        self.logger.info(f"Scraper CORREGIDO iniciado para {self.modelo_config['nombre']}")
    
    def setup_driver(self) -> webdriver.Chrome:
        """Configurar driver Chrome optimizado"""
        options = Options()
        
        # Configuraciones de rendimiento
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-client-side-phishing-detection")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-translate")
        options.add_argument("--hide-scrollbars")
        options.add_argument("--mute-audio")
        
        # Suprimir logs
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # User agent
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # GitHub Actions
        if is_github_actions():
            options.add_argument("--headless")
        
        # Configuraciones de rendimiento
        prefs = {
            "profile.default_content_setting_values": {
                "images": 2,
                "plugins": 2,
                "popups": 2,
                "geolocation": 2,
                "notifications": 2,
                "media_stream": 2,
            }
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            driver = webdriver.Chrome(options=options)
            
            # Timeouts optimizados
            driver.implicitly_wait(1.0)
            driver.set_page_load_timeout(20)
            driver.set_script_timeout(15)
            
            if not is_github_actions():
                driver.maximize_window()
            
            self.logger.info("Driver configurado correctamente")
            return driver
            
        except Exception as e:
            self.logger.error(f"Error configurando driver: {e}")
            raise
    
    def scrape_model(self) -> pd.DataFrame:
        """Método principal para scraping con debug mejorado"""
        try:
            self.logger.info(f"Iniciando scraping CORREGIDO de {self.modelo_config['nombre']}")
            
            # Configurar driver
            self.driver = self.setup_driver()
            
            # Obtener URLs
            search_urls = self.get_search_urls()
            self.logger.info(f"{len(search_urls)} URLs de búsqueda generadas")
            
            # Timeouts
            start_time = time.time()
            max_total_time = 14400 if is_github_actions() else 18000  # 4-5 horas
            max_urls_to_process = 25 if is_github_actions() else 50
            
            # Procesar URLs
            urls_to_process = search_urls[:max_urls_to_process]
            self.logger.info(f"Procesando {len(urls_to_process)} URLs de {len(search_urls)} totales")
            
            for i, url in enumerate(urls_to_process, 1):
                # Verificar tiempo
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    self.logger.warning(f"Tiempo límite alcanzado")
                    break
                
                self.logger.info(f"[{i}/{len(urls_to_process)}] Procesando URL: {url[:80]}...")
                
                try:
                    motos_esta_url = self.process_search_url_corrected(url, i)
                    self.debug_counters['urls_procesadas'] += 1
                    self.logger.info(f"[{i}/{len(urls_to_process)}] RESULTADO: {motos_esta_url} motos extraídas")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.warning(f"Error procesando URL {i}: {e}")
                    continue
            
            # Resultado final
            if self.results:
                df = pd.DataFrame(self.results)
                self.logger.info(f"SCRAPING COMPLETADO: {len(df)} motos encontradas")
                self._show_debug_summary()
                return df
            else:
                self.logger.warning("No se encontraron resultados")
                self._show_debug_summary()
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error durante scraping: {e}")
            return pd.DataFrame()
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info("Driver cerrado correctamente")
                except:
                    pass
    
    def process_search_url_corrected(self, url: str, url_number: int) -> int:
        """Procesar URL con manejo de errores mejorado"""
        max_url_time = 900  # 15 minutos por URL
        start_url_time = time.time()
        motos_esta_url = 0
        
        try:
            # Navegar
            self.driver.get(url)
            time.sleep(3)
            
            # Aceptar cookies
            self._accept_cookies()
            
            # Cargar más anuncios
            total_enlaces = self._smart_load_ads()
            self.debug_counters['enlaces_encontrados'] += total_enlaces
            
            if total_enlaces == 0:
                self.logger.warning(f"[URL {url_number}] No se encontraron enlaces")
                return 0
            
            # Obtener enlaces finales
            enlaces = self._get_anuncio_links()
            
            if not enlaces:
                self.logger.warning(f"[URL {url_number}] No se pudieron extraer enlaces")
                return 0
            
            self.logger.info(f"[URL {url_number}] Procesando {len(enlaces)} enlaces")
            
            # Procesar anuncios
            motos_esta_url = self._process_anuncios_corrected(enlaces, url_number)
            
            return motos_esta_url
                    
        except Exception as e:
            self.logger.error(f"Error procesando URL: {e}")
            return 0
    
    def _accept_cookies(self):
        """Acepta cookies rápidamente"""
        try:
            cookie_button = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            cookie_button.click()
            time.sleep(1)
        except:
            pass
    
    def _smart_load_ads(self) -> int:
        """Carga anuncios usando el botón 'Cargar más'"""
        target_count = 50
        max_clicks = 5
        
        # Scroll inicial
        for i in range(3):
            self.driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(0.5)
        
        initial_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        clicks_realizados = 0
        last_count = initial_count
        
        for click_num in range(max_clicks):
            # Scroll al final
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            if self._click_load_more():
                clicks_realizados += 1
                time.sleep(2)
                
                new_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
                
                if new_count > last_count:
                    self.logger.info(f"[SMART] Clic {clicks_realizados}: {last_count} -> {new_count} (+{new_count - last_count})")
                    last_count = new_count
                    
                    if new_count >= target_count:
                        self.logger.info(f"[SMART] Objetivo alcanzado: {new_count} anuncios")
                        break
                else:
                    self.logger.info(f"[SMART] Sin nuevos anuncios, fin del contenido")
                    break
            else:
                self.logger.info(f"[SMART] Botón no encontrado, fin del contenido")
                break
        
        final_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        self.logger.info(f"[SMART] Total final: {final_count} anuncios ({clicks_realizados} clics)")
        
        return final_count
    
    def _click_load_more(self) -> bool:
        """Busca y hace clic en 'Cargar más'"""
        selectors = [
            ('css', 'walla-button[text="Cargar más"]'),
            ('css', 'walla-button[text*="Cargar"]'),
            ('css', 'button.walla-button__button'),
            ('xpath', '//span[text()="Cargar más"]/ancestor::button'),
            ('xpath', '//button[contains(@class, "walla-button")]')
        ]
        
        for selector_type, selector in selectors:
            try:
                if selector_type == 'css':
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                else:
                    elements = self.driver.find_elements(By.XPATH, selector)
                
                for element in elements:
                    try:
                        if element.is_displayed() and element.is_enabled():
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(0.5)
                            
                            try:
                                element.click()
                                self.logger.info(f"Clic exitoso en 'Cargar más'")
                                return True
                            except:
                                self.driver.execute_script("arguments[0].click();", element)
                                self.logger.info(f"Clic con JS exitoso")
                                return True
                    except:
                        continue
            except:
                continue
        
        return False
    
    def _get_anuncio_links(self) -> List[str]:
        """Obtener enlaces de anuncios"""
        enlaces = []
        
        try:
            # Selectores más específicos
            link_selectors = [
                'a[href*="/item/"]'
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
            
            return list(set(enlaces))
            
        except Exception as e:
            self.logger.error(f"Error obteniendo enlaces: {e}")
            return []
    
    def _process_anuncios_corrected(self, enlaces: List[str], url_number: int) -> int:
        """Procesar anuncios con extracción corregida"""
        anuncios_procesados = 0
        max_anuncios = 30
        
        enlaces_a_procesar = enlaces[:max_anuncios]
        
        for i, enlace in enumerate(tqdm(enlaces_a_procesar, desc=f"URL {url_number}"), 1):
            if enlace in self.processed_urls:
                continue
            
            try:
                # Navegar al anuncio
                self.driver.get(enlace)
                time.sleep(1)
                
                # Extraer datos CORREGIDOS
                moto_data = self._extract_anuncio_data_corrected(enlace)
                self.debug_counters['anuncios_procesados'] += 1
                
                if moto_data:
                    self.debug_counters['datos_extraidos'] += 1
                    
                    # DEBUG: Mostrar datos extraídos para los primeros 3
                    if self.debug_counters['datos_extraidos'] <= 3:
                        self.logger.info(f"[DATOS {self.debug_counters['datos_extraidos']}] Extraídos:")
                        self.logger.info(f"  Titulo: '{moto_data.get('Titulo', 'N/A')}'")
                        self.logger.info(f"  Precio: '{moto_data.get('Precio', 'N/A')}'")
                        self.logger.info(f"  Kilometraje: '{moto_data.get('Kilometraje', 'N/A')}'")
                        self.logger.info(f"  Año: '{moto_data.get('Año', 'N/A')}'")
                    
                    if self.validate_moto_data(moto_data):
                        self.results.append(moto_data)
                        self.processed_urls.add(enlace)
                        anuncios_procesados += 1
                        self.debug_counters['validaciones_exitosas'] += 1
                else:
                    self.debug_counters['errores_extraccion'] += 1
                
            except Exception as e:
                self.debug_counters['errores_extraccion'] += 1
                continue
        
        return anuncios_procesados
    
    def _extract_anuncio_data_corrected(self, url: str) -> Optional[Dict]:
        """CORREGIDO: Extracción de datos con nombres consistentes"""
        try:
            # USAR NOMBRES CONSISTENTES (sin tildes)
            data = {
                'URL': url,
                'Titulo': self._extract_titulo_corrected(),           # SIN TILDE
                'Precio': self._extract_precio_corrected(),          
                'Kilometraje': self._extract_kilometraje_corrected(), 
                'Año': self._extract_año_corrected(),                 
                'Vendedor': self._extract_vendedor_corrected(),       
                'Ubicacion': self._extract_ubicacion_corrected(),     
                'Fecha_Publicacion': self._extract_fecha_publicacion_corrected(),
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo datos de {url}: {e}")
            return None
    
    def _extract_titulo_corrected(self) -> str:
        """CORREGIDO: Extractor de título mejorado"""
        try:
            # Esperar a que cargue el título
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
            except:
                pass
            
            # Selectores para título
            selectors = [
                'h1[data-testid="item-title"]',
                'h1.item-detail_title',
                'h1[class*="title"]',
                'h1[class*="Title"]',
                'h1',
                '*[class*="title"] h1',
                '*[class*="Title"] h1'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        title = element.text.strip()
                        if len(title) > 5:
                            return title
                except:
                    continue
            
            return "Sin titulo"
            
        except Exception as e:
            return "Sin titulo"
    
    def _extract_precio_corrected(self) -> str:
        """CORREGIDO: Extractor de precio mejorado"""
        try:
            # Esperar a que carguen los precios
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
                )
            except:
                pass
            
            # Selectores específicos para precios
            price_selectors = [
                "span.item-detail-price_ItemDetailPrice--standardFinanced__f9ceG",
                "*[class*='ItemDetailPrice']",
                "*[class*='item-price']",
                "*[class*='price'] span"
            ]
            
            for selector in price_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        if text and '€' in text:
                            price = self._extract_price_from_text(text)
                            if price != "No especificado":
                                return price
                except:
                    continue
            
            # Búsqueda general de precios
            try:
                price_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
                for elem in price_elements[:5]:
                    text = elem.text.strip()
                    if text and 5 <= len(text) <= 20:
                        price = self._extract_price_from_text(text)
                        if price != "No especificado":
                            return price
            except:
                pass
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _extract_price_from_text(self, text: str) -> str:
        """Extraer precio de texto"""
        if not text:
            return "No especificado"
        
        clean_text = text.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
        
        # Patrones de precio
        price_patterns = [
            r'(\d{1,3}(?:\.\d{3})+)\s*€',  # 7.690 €
            r'(\d{4,6})\s*€',              # 7690 €
            r'(\d{1,2}\.\d{3})\s*€'        # 7.690 €
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, clean_text)
            if match:
                try:
                    price_str = match.group(1).replace('.', '')
                    price_value = int(price_str)
                    if 100 <= price_value <= 100000:
                        return f"{price_value:,} €".replace(',', '.')
                except:
                    continue
        
        return "No especificado"
    
    def _extract_kilometraje_corrected(self) -> str:
        """CORREGIDO: Extractor de kilometraje mejorado"""
        try:
            # Buscar en el contenido de la página
            page_source = self.driver.page_source.lower()
            
            # Patrones de kilometraje
            km_patterns = [
                r'kilómetros[:\s]*(\d{1,3}\.?\d{0,3})',
                r'(\d{1,3}\.?\d{0,3})\s*km\b',
                r'(\d{1,3}\.?\d{0,3})\s*kilómetros?\b'
            ]
            
            for pattern in km_patterns:
                matches = re.finditer(pattern, page_source)
                for match in matches:
                    try:
                        km_str = match.group(1).replace('.', '')
                        km_value = int(km_str)
                        if 0 <= km_value <= 500000:
                            if km_value == 0:
                                return "0 km"
                            elif km_value < 1000:
                                return f"{km_value} km"
                            else:
                                return f"{km_value:,} km".replace(',', '.')
                    except:
                        continue
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _extract_año_corrected(self) -> str:
        """CORREGIDO: Extractor de año mejorado"""
        try:
            # Obtener título y buscar año en él
            titulo = self._extract_titulo_corrected()
            combined_text = f"{titulo} {self.driver.page_source}"
            
            # Patrones de año
            year_patterns = [
                r'\b(20[1-2][0-9])\b(?!\s*(?:€|euros|km|cc|cv))',
                r'año[:\s]*(20[1-2][0-9])',
                r'modelo[:\s]*(20[1-2][0-9])'
            ]
            
            # Buscar primero en título
            for pattern in year_patterns:
                match = re.search(pattern, titulo.lower())
                if match:
                    year = int(match.group(1))
                    if 2010 <= year <= 2025:
                        return str(year)
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _extract_vendedor_corrected(self) -> str:
        """CORREGIDO: Extractor de vendedor mejorado"""
        try:
            selectors = [
                'h3[data-testid="seller-name"]',
                'h3.seller-name',
                '*[class*="seller"] h3',
                '*[class*="user"] h3',
                'h3'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        vendedor_text = element.text.strip()
                        
                        # Detectar comerciales
                        commercial_keywords = ['concesionario', 'taller', 'motor', 's.l.', 'comercial']
                        vendedor_lower = vendedor_text.lower()
                        
                        if any(keyword in vendedor_lower for keyword in commercial_keywords):
                            return f"Comercial: {vendedor_text}"
                        
                        return vendedor_text
                except:
                    continue
            
            return "Particular"
            
        except Exception as e:
            return "Particular"
    
    def _extract_ubicacion_corrected(self) -> str:
        """CORREGIDO: Extractor de ubicación mejorado"""
        try:
            selectors = [
                'a[data-testid="location"]',
                '*[class*="location"] a',
                '*[class*="Location"] a'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        ubicacion = element.text.strip()
                        return ubicacion.replace("en ", "").replace("En ", "")
                except:
                    continue
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _extract_fecha_publicacion_corrected(self) -> str:
        """CORREGIDO: Extractor de fecha mejorado"""
        try:
            selectors = [
                'span[data-testid="publication-date"]',
                '*[class*="date"]',
                '*[class*="time"]'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        return element.text.strip()
                except:
                    continue
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _show_debug_summary(self):
        """Mostrar resumen de debug"""
        counters = self.debug_counters
        
        self.logger.info("="*60)
        self.logger.info("RESUMEN DEBUG BASE SCRAPER")
        self.logger.info("="*60)
        self.logger.info(f"URLs procesadas: {counters['urls_procesadas']}")
        self.logger.info(f"Enlaces encontrados: {counters['enlaces_encontrados']}")
        self.logger.info(f"Anuncios procesados: {counters['anuncios_procesados']}")
        self.logger.info(f"Datos extraídos: {counters['datos_extraidos']}")
        self.logger.info(f"Validaciones exitosas: {counters['validaciones_exitosas']}")
        self.logger.info(f"Errores de extracción: {counters['errores_extraccion']}")
        
        if counters['anuncios_procesados'] > 0:
            exito_extraccion = (counters['datos_extraidos'] / counters['anuncios_procesados']) * 100
            exito_validacion = (counters['validaciones_exitosas'] / counters['datos_extraidos']) * 100 if counters['datos_extraidos'] > 0 else 0
            
            self.logger.info(f"Tasa éxito extracción: {exito_extraccion:.1f}%")
            self.logger.info(f"Tasa éxito validación: {exito_validacion:.1f}%")
        
        self.logger.info("="*60)
    
    @abstractmethod
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda específicas del modelo"""
        pass
    
    @abstractmethod
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos de la moto corresponden al modelo específico"""
        pass
