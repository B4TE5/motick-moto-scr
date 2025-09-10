"""
================================================================================
                                BASE SCRAPER CORREGIDO                     
================================================================================

Autor: Carlos Peraza
Version: 3.0 
Fecha: Septiembre 2025

CORRECCIONES:
- Selectores CSS actualizados para Wallapop 2025
- Implementacion correcta del boton "Cargar mas"
- Extraccion de datos mejorada y precisa
- Navegacion profunda por todas las paginas

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

from config import get_modelo_config, SELENIUM_CONFIG, is_github_actions

class BaseScraper(ABC):
    """Clase base CORREGIDA para scrapers de modelos especificos de motos"""
    
    def __init__(self, modelo_key: str):
        """Inicializar scraper base"""
        self.modelo_key = modelo_key
        self.modelo_config = get_modelo_config(modelo_key)
        self.selenium_config = SELENIUM_CONFIG
        self.logger = logging.getLogger(__name__)
        
        self.driver = None
        self.results = []
        self.processed_urls = set()
        
        self.logger.info(f"Scraper iniciado para {self.modelo_config['nombre']}")
    
    def setup_driver(self) -> webdriver.Chrome:
        """Configurar y retornar driver de Chrome optimizado"""
        options = Options()
        
        # Configuracion basica
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
        options.add_argument("--disable-backgrounding-occluded-windows")
        
        # User agent actualizado
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Configuracion para GitHub Actions
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
            
            # TIMEOUTS EXTENDIDOS
            driver.implicitly_wait(3.0)
            driver.set_page_load_timeout(45)
            driver.set_script_timeout(30)
            
            if not is_github_actions():
                driver.maximize_window()
            
            self.logger.info("Driver configurado correctamente")
            return driver
            
        except Exception as e:
            self.logger.error(f"Error configurando driver: {e}")
            raise
    
    def scrape_model(self) -> pd.DataFrame:
        """Metodo principal para hacer scraping de un modelo"""
        try:
            self.logger.info(f"Iniciando scraping CORREGIDO de {self.modelo_config['nombre']}")
            
            # Configurar driver
            self.driver = self.setup_driver()
            
            # Obtener URLs de busqueda especificas del modelo
            search_urls = self.get_search_urls()
            self.logger.info(f"{len(search_urls)} URLs de busqueda generadas")
            
            # TIMEOUTS EXTENDIDOS PARA EJECUCION COMPLETA
            start_time = time.time()
            max_total_time = 14400 if is_github_actions() else 18000  # 4-5 horas
            
            for i, url in enumerate(search_urls, 1):
                # Verificar tiempo total
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    self.logger.warning(f"Tiempo limite alcanzado ({max_total_time/3600:.1f} horas)")
                    break
                
                self.logger.info(f"Procesando URL {i}/{len(search_urls)}: {url[:80]}...")
                
                try:
                    self.process_search_url_with_load_more(url)
                    time.sleep(3)
                except Exception as e:
                    self.logger.warning(f"Error procesando URL {i}: {e}")
                    continue
            
            # Convertir resultados a DataFrame
            if self.results:
                df = pd.DataFrame(self.results)
                self.logger.info(f"Scraping completado: {len(df)} motos encontradas")
                
                # DEBUG: Mostrar datos extraidos
                self._debug_extracted_data(df)
                
                return df
            else:
                self.logger.warning("No se encontraron resultados")
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
    
    def process_search_url_with_load_more(self, url: str):
        """CORREGIDO: Procesar URL con implementacion del boton 'Cargar mas'"""
        max_url_time = 2400 if is_github_actions() else 3600  # 40min-1h por URL
        start_url_time = time.time()
        
        try:
            # Cargar pagina
            self.driver.get(url)
            time.sleep(5)
            
            # Scroll inicial para cargar contenido
            self.initial_scroll_and_wait()
            
            # Implementar ciclo de "Cargar mas"
            total_anuncios_procesados = 0
            max_clicks_cargar_mas = 20  # Maximo 20 clics en "Cargar mas"
            
            for click_iteration in range(max_clicks_cargar_mas):
                # Verificar timeout de URL
                elapsed_url_time = time.time() - start_url_time
                if elapsed_url_time > max_url_time:
                    self.logger.warning(f"Timeout de URL alcanzado")
                    break
                
                # Obtener enlaces de anuncios actuales
                enlaces = self.get_anuncio_links_corrected()
                
                if not enlaces:
                    self.logger.warning(f"No se encontraron enlaces en iteracion {click_iteration + 1}")
                    break
                
                self.logger.info(f"Iteracion {click_iteration + 1}: {len(enlaces)} enlaces encontrados")
                
                # Procesar anuncios de esta iteracion
                anuncios_procesados_iteracion = self.process_anuncios_batch(enlaces, start_url_time, max_url_time)
                total_anuncios_procesados += anuncios_procesados_iteracion
                
                # Intentar hacer clic en "Cargar mas"
                if not self.click_load_more_button():
                    self.logger.info(f"No hay mas contenido para cargar o boton no encontrado")
                    break
                
                # Esperar a que cargue nuevo contenido
                time.sleep(4)
            
            self.logger.info(f"URL procesada: {total_anuncios_procesados} motos validas en {click_iteration + 1} iteraciones")
                    
        except Exception as e:
            self.logger.error(f"Error procesando URL de busqueda: {e}")
    
    def initial_scroll_and_wait(self):
        """Scroll inicial para cargar el contenido base"""
        try:
            # Scroll gradual para activar lazy loading
            for i in range(3):
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {(i+1)/3});")
                time.sleep(2)
            
            # Volver arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
        except Exception as e:
            self.logger.warning(f"Error durante scroll inicial: {e}")
    
    def click_load_more_button(self) -> bool:
        """NUEVO: Implementacion del boton 'Cargar mas'"""
        try:
            # Scroll hasta abajo para ver el boton
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            
            # SELECTORES PARA EL BOTON "CARGAR MAS"
            load_more_selectors = [
                # Selector principal basado en la estructura que proporcionaste
                'walla-button[text="Cargar más"]',
                'button:contains("Cargar más")',
                '.search-page-results_SearchPageResults__loadMore__A_eRR button',
                'walla-button[button-type="primary"]',
                '.walla-button__button:contains("Cargar más")',
                # Selectores fallback
                'button[type="button"]:contains("Cargar")',
                '*[class*="loadMore"] button',
                '*[class*="load-more"] button',
                '*[text*="Cargar"]',
                'button[aria-label*="Cargar"]'
            ]
            
            for selector in load_more_selectors:
                try:
                    if 'contains' in selector:
                        # Para selectores con :contains usar XPath
                        xpath = f"//button[contains(text(), 'Cargar más') or contains(text(), 'Cargar') or contains(text(), 'Ver más')]"
                        elements = self.driver.find_elements(By.XPATH, xpath)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            # Scroll al elemento
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(1)
                            
                            # Hacer clic
                            try:
                                element.click()
                                self.logger.info(f"Clic exitoso en 'Cargar mas' con selector: {selector}")
                                return True
                            except:
                                # Intentar clic con JavaScript
                                self.driver.execute_script("arguments[0].click();", element)
                                self.logger.info(f"Clic con JS exitoso en 'Cargar mas'")
                                return True
                                
                except Exception as e:
                    continue
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Error buscando boton 'Cargar mas': {e}")
            return False
    
    def process_anuncios_batch(self, enlaces: List[str], start_url_time: float, max_url_time: float) -> int:
        """Procesar un lote de anuncios con control de tiempo"""
        anuncios_procesados = 0
        max_anuncios_por_lote = 50 if is_github_actions() else 100
        
        for i, enlace in enumerate(enlaces[:max_anuncios_por_lote], 1):
            # Verificar timeout
            elapsed_url_time = time.time() - start_url_time
            if elapsed_url_time > max_url_time:
                self.logger.warning(f"Timeout alcanzado durante procesamiento de lote")
                break
            
            if enlace in self.processed_urls:
                continue
            
            try:
                moto_data = self.extract_anuncio_data_corrected(enlace)
                
                if moto_data and self.validate_moto_data(moto_data):
                    self.results.append(moto_data)
                    self.processed_urls.add(enlace)
                    anuncios_procesados += 1
                    self.logger.debug(f"Moto valida: {moto_data.get('Titulo', 'Sin titulo')[:30]}")
                
                time.sleep(1.5)
                
            except Exception as e:
                self.logger.warning(f"Error extrayendo anuncio {i}: {e}")
                continue
        
        return anuncios_procesados
    
    def get_anuncio_links_corrected(self) -> List[str]:
        """CORREGIDO: Obtener enlaces con selectores actualizados"""
        enlaces = []
        
        try:
            # SELECTORES ACTUALIZADOS PARA WALLAPOP 2025
            link_selectors = [
                # Selectores principales actualizados
                'a[href*="/item/"]',
                'a[href*="/app/user/"]',
                '*[class*="ItemCard"] a',
                '*[class*="item-card"] a',
                '*[class*="Card"] a[href*="/item/"]',
                # Selectores mas genericos
                'a[href*="wallapop.com/item/"]',
                'a[data-testid*="item"]',
                '.tsl-item-card a',
                '*[role="link"][href*="/item/"]'
            ]
            
            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and self.is_valid_anuncio_link(href) and href not in enlaces:
                            enlaces.append(href)
                except:
                    continue
            
            # Eliminar duplicados manteniendo orden
            enlaces_unicos = []
            seen = set()
            for enlace in enlaces:
                if enlace not in seen:
                    enlaces_unicos.append(enlace)
                    seen.add(enlace)
            
            return enlaces_unicos
            
        except Exception as e:
            self.logger.error(f"Error obteniendo enlaces: {e}")
            return []
    
    def extract_anuncio_data_corrected(self, url: str) -> Optional[Dict]:
        """CORREGIDO: Extraccion de datos con selectores actualizados"""
        try:
            # Cargar pagina del anuncio
            self.driver.get(url)
            time.sleep(4)
            
            # EXTRAER DATOS CON SELECTORES CORREGIDOS
            data = {
                'URL': url,
                'Titulo': self.extract_titulo_corrected(),           
                'Precio': self.extract_precio_corrected(),          
                'Kilometraje': self.extract_kilometraje_corrected(), 
                'Año': self.extract_año_corrected(),                 
                'Vendedor': self.extract_vendedor_corrected(),       
                'Ubicacion': self.extract_ubicacion_corrected(),     
                'Fecha_Publicacion': self.extract_fecha_publicacion_corrected(),
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            self.logger.warning(f"Error extrayendo datos de {url}: {e}")
            return None
    
    def extract_titulo_corrected(self) -> str:
        """CORREGIDO: Titulo con selectores actualizados"""
        try:
            # Esperar a que cargue
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            # SELECTORES ACTUALIZADOS PARA TITULO
            selectors = [
                'h1[data-testid="item-title"]',
                'h1[class*="item-detail-title"]',
                'h1[class*="ItemDetail"]',
                'h1[class*="Title"]',
                '.item-detail h1',
                'h1',
                '*[class*="title"] h1'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        title = element.text.strip()
                        if len(title) > 3:
                            return title
                except:
                    continue
            
            return "Sin titulo"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo titulo: {e}")
            return "Sin titulo"
    
    def extract_precio_corrected(self) -> str:
        """CORREGIDO: Precio con selectores actualizados"""
        try:
            # ESPERAR A QUE CARGUEN LOS PRECIOS
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
                )
            except:
                pass
            
            # SELECTORES ACTUALIZADOS PARA PRECIO
            price_selectors = [
                '*[class*="item-price"]',
                '*[class*="ItemPrice"]',
                '*[class*="price"] span',
                '*[data-testid*="price"]',
                'span[class*="price"]',
                '.price',
                '*[class*="Price"]'
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
            
            # BUSQUEDA GENERICA DE PRECIOS
            try:
                price_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
                
                for elem in price_elements[:5]:
                    try:
                        text = elem.text.strip()
                        if text and len(text) < 50:  # Evitar textos muy largos
                            price = self._extract_price_from_text(text)
                            if price != "No especificado":
                                return price
                    except:
                        continue
                        
            except:
                pass
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo precio: {e}")
            return "No especificado"
    
    def extract_kilometraje_corrected(self) -> str:
        """CORREGIDO: Kilometraje con busqueda mejorada"""
        try:
            # Obtener todo el texto de la pagina
            page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()
            
            # PATRONES ACTUALIZADOS PARA KILOMETRAJE
            km_patterns = [
                r'(\d{1,3}(?:\.\d{3})*)\s*km\b',
                r'(\d{1,6})\s*km\b',
                r'(\d{1,3}(?:\.\d{3})*)\s*kilómetros?\b',
                r'kilometraje[:\s]*(\d{1,6})',
                r'km[:\s]*(\d{1,6})',
                r'(\d{1,3}),(\d{3})\s*km',
                r'(\d+)\s*\.?\s*(\d{3})\s*km'
            ]
            
            km_candidates = []
            
            for pattern in km_patterns:
                matches = re.finditer(pattern, page_text)
                for match in matches:
                    try:
                        if len(match.groups()) == 2:  # Formato con separador
                            km_value = int(match.group(1) + match.group(2))
                        else:
                            km_str = match.group(1).replace('.', '').replace(',', '')
                            km_value = int(km_str)
                        
                        # Validar rango razonable
                        if 0 <= km_value <= 200000:
                            km_candidates.append(km_value)
                    except:
                        continue
            
            if km_candidates:
                # Tomar el kilometraje mas comun o el mas razonable
                km_final = max(set(km_candidates), key=km_candidates.count)
                
                if km_final == 0:
                    return "0 km"
                elif km_final < 1000:
                    return f"{km_final} km"
                else:
                    return f"{km_final:,} km".replace(',', '.')
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo kilometraje: {e}")
            return "No especificado"
    
    def extract_año_corrected(self) -> str:
        """CORREGIDO: Año con busqueda mejorada"""
        try:
            # Obtener titulo y contenido
            titulo = self.extract_titulo_corrected()
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Combinar textos
            combined_text = f"{titulo} {page_text}".lower()
            
            # PATRONES ACTUALIZADOS PARA AÑO
            year_patterns = [
                r'\b(20[1-2][0-9])\b',
                r'año[:\s]*(20[1-2][0-9])',
                r'modelo[:\s]*(20[1-2][0-9])',
                r'del[:\s]*(20[1-2][0-9])',
                r'matriculad[ao][:\s]*(20[1-2][0-9])'
            ]
            
            year_candidates = []
            
            # Buscar primero en el titulo
            for pattern in year_patterns:
                matches = re.finditer(pattern, titulo.lower())
                for match in matches:
                    try:
                        year = int(match.group(1))
                        if 2010 <= year <= 2025:
                            year_candidates.append((year, 10))  # Peso alto para titulo
                    except:
                        continue
            
            # Buscar en el contenido general
            for pattern in year_patterns:
                matches = re.finditer(pattern, combined_text)
                for match in matches:
                    try:
                        year = int(match.group(1))
                        if 2010 <= year <= 2025:
                            year_candidates.append((year, 1))  # Peso normal
                    except:
                        continue
            
            if year_candidates:
                # Ordenar por peso y tomar el mejor
                year_candidates.sort(key=lambda x: x[1], reverse=True)
                return str(year_candidates[0][0])
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo año: {e}")
            return "No especificado"
    
    def extract_vendedor_corrected(self) -> str:
        """CORREGIDO: Vendedor con selectores actualizados"""
        try:
            # SELECTORES ACTUALIZADOS PARA VENDEDOR
            selectors = [
                '*[data-testid*="user"]',
                '*[data-testid*="seller"]',
                '*[class*="seller"] *[class*="name"]',
                '*[class*="user"] h3',
                '*[class*="profile"] h3',
                'h3',
                '*[class*="UserName"]'
            ]
            
            vendedor_text = ""
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        vendedor_text = element.text.strip()
                        break
                except:
                    continue
            
            if not vendedor_text:
                return "Particular"
            
            # DETECCION DE COMERCIALES
            vendedor_lower = vendedor_text.lower()
            
            commercial_keywords = [
                's.l.', 'sl', 's.a.', 'sa', 's.l.u.', 'slu',
                'concesionario', 'taller', 'motor', 'moto', 'auto',
                'honda', 'yamaha', 'kawasaki', 'suzuki', 'bmw',
                'comercial', 'venta', 'ventas'
            ]
            
            for keyword in commercial_keywords:
                if keyword in vendedor_lower:
                    return f"Comercial"
            
            return vendedor_text
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo vendedor: {e}")
            return "Particular"
    
    def extract_ubicacion_corrected(self) -> str:
        """CORREGIDO: Ubicacion con selectores actualizados"""
        try:
            selectors = [
                '*[data-testid*="location"]',
                '*[class*="location"]',
                '*[class*="Location"]',
                'a[href*="latitude"]',
                '*[class*="address"]'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        ubicacion = element.text.strip()
                        # Limpiar prefijos
                        ubicacion = ubicacion.replace("en ", "").replace("En ", "")
                        if len(ubicacion) > 2:
                            return ubicacion
                except:
                    continue
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo ubicacion: {e}")
            return "No especificado"
    
    def extract_fecha_publicacion_corrected(self) -> str:
        """CORREGIDO: Fecha de publicacion"""
        try:
            # Buscar en todo el texto
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            
            # Patrones de fecha
            date_patterns = [
                r'hace (\d+) días?',
                r'hace (\d+) horas?',
                r'hace (\d+) minutos?',
                r'ayer',
                r'hoy',
                r'\d{1,2}/\d{1,2}/\d{4}',
                r'\d{1,2}-\d{1,2}-\d{4}'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, page_text.lower())
                if match:
                    return match.group(0)
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo fecha: {e}")
            return "No especificado"
    
    def _extract_price_from_text(self, text: str) -> str:
        """Extraer precio de texto"""
        if not text:
            return "No especificado"
        
        clean_text = text.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
        
        price_patterns = [
            r'(\d{1,3}(?:\.\d{3})+)\s*€',
            r'(\d{4,6})\s*€',
            r'(\d{1,2})\s*\.\s*(\d{3})\s*€',
            r'(\d{1,2}),(\d{3})\s*€',
            r'€\s*(\d{1,2}\.?\d{3,6})',
            r'(\d{1,2}\.?\d{3,6})\s*euros?'
        ]
        
        for pattern in price_patterns:
            matches = re.finditer(pattern, clean_text, re.IGNORECASE)
            for match in matches:
                try:
                    if len(match.groups()) == 2:
                        price_value = int(match.group(1) + match.group(2))
                    else:
                        price_str = match.group(1).replace('.', '').replace(',', '')
                        price_value = int(price_str)
                    
                    if 100 <= price_value <= 100000:
                        return f"{price_value:,} €".replace(',', '.')
                except:
                    continue
        
        return "No especificado"
    
    def is_valid_anuncio_link(self, url: str) -> bool:
        """Validar si un enlace es valido para procesar"""
        if not url or '/item/' not in url:
            return False
        
        excluded_patterns = [
            'promoted',
            'destacado', 
            'premium',
            'banner',
            '/app/user/'  # Enlaces de usuario, no de producto
        ]
        
        for pattern in excluded_patterns:
            if pattern in url.lower():
                return False
        
        return True
    
    def _debug_extracted_data(self, df: pd.DataFrame):
        """Debug para mostrar datos extraidos"""
        print("\n=== DEBUG: DATOS EXTRAIDOS CORREGIDOS ===")
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. Titulo: '{row.get('Titulo', 'N/A')}'")
            print(f"   Precio: '{row.get('Precio', 'N/A')}'")
            print(f"   Kilometraje: '{row.get('Kilometraje', 'N/A')}'")
            print(f"   Año: '{row.get('Año', 'N/A')}'")
            print(f"   Vendedor: '{row.get('Vendedor', 'N/A')}'")
            print(f"   Ubicacion: '{row.get('Ubicacion', 'N/A')}'")
        print("==========================================\n")
    
    @abstractmethod
    def get_search_urls(self) -> List[str]:
        """Generar URLs de busqueda especificas del modelo"""
        pass
    
    @abstractmethod
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos de la moto corresponden al modelo especifico"""
        pass
