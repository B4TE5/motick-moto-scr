"""
================================================================================
                                BASE SCRAPER                     
================================================================================

Autor: Carlos Peraza
Versión: 2.0 
Fecha: Agosto 2025

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
            
            # TIMEOUTS EXTENDIDOS PARA MAS TIEMPO DE SCRAPING
            driver.implicitly_wait(2.0)
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(20)
            
            if not is_github_actions():
                driver.maximize_window()
            
            self.logger.info("Driver configurado correctamente")
            return driver
            
        except Exception as e:
            self.logger.error(f"Error configurando driver: {e}")
            raise
    
    def scrape_model(self) -> pd.DataFrame:
        """Método principal para hacer scraping de un modelo"""
        try:
            self.logger.info(f"Iniciando scraping EXTENDIDO de {self.modelo_config['nombre']}")
            
            # Configurar driver
            self.driver = self.setup_driver()
            
            # Obtener URLs de búsqueda específicas del modelo
            search_urls = self.get_search_urls()
            self.logger.info(f"{len(search_urls)} URLs de búsqueda generadas")
            
            # TIMEOUTS EXTENDIDOS PARA EJECUCION COMPLETA
            start_time = time.time()
            max_total_time = 14400 if is_github_actions() else 18000  # 4-5 horas
            
            for i, url in enumerate(search_urls, 1):
                # Verificar tiempo total
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    self.logger.warning(f"Tiempo límite alcanzado ({max_total_time/3600:.1f} horas)")
                    break
                
                self.logger.info(f"Procesando URL {i}/{len(search_urls)}: {url[:80]}...")
                
                try:
                    self.process_search_url_extended(url)
                    time.sleep(2)
                except Exception as e:
                    self.logger.warning(f"Error procesando URL {i}: {e}")
                    continue
            
            # Convertir resultados a DataFrame
            if self.results:
                df = pd.DataFrame(self.results)
                self.logger.info(f"Scraping completado: {len(df)} motos encontradas")
                
                # DEBUG: Mostrar datos extraídos
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
    
    def _debug_extracted_data(self, df: pd.DataFrame):
        """Debug para mostrar datos extraídos"""
        print("\n=== DEBUG: DATOS EXTRAIDOS ===")
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. Título: '{row.get('Título', 'N/A')}'")
            print(f"   Precio: '{row.get('Precio', 'N/A')}'")
            print(f"   Kilometraje: '{row.get('Kilometraje', 'N/A')}'")
            print(f"   Año: '{row.get('Año', 'N/A')}'")
            print(f"   Vendedor: '{row.get('Vendedor', 'N/A')}'")
        print("================================\n")
    
    def process_search_url_extended(self, url: str):
        """Procesar una URL de búsqueda con tiempo extendido"""
        max_url_time = 2400 if is_github_actions() else 3600  # 40min-1h por URL
        start_url_time = time.time()
        
        try:
            # Cargar página con timeout
            self.driver.get(url)
            time.sleep(3)
            
            # Obtener enlaces de anuncios
            enlaces = self.get_anuncio_links_enhanced()
            
            if not enlaces:
                self.logger.warning(f"No se encontraron enlaces en esta URL")
                return
            
            self.logger.info(f"{len(enlaces)} enlaces encontrados")
            
            # PROCESAR MUCHOS MAS ANUNCIOS
            max_anuncios = 50 if is_github_actions() else len(enlaces)  # SIN LIMITE AGRESIVO
            anuncios_procesados = 0
            
            for i, enlace in enumerate(enlaces[:max_anuncios], 1):
                # Verificar timeout de URL
                elapsed_url_time = time.time() - start_url_time
                if elapsed_url_time > max_url_time:
                    self.logger.warning(f"Timeout de URL alcanzado")
                    break
                
                if enlace in self.processed_urls:
                    continue
                
                try:
                    self.logger.debug(f"Procesando anuncio {i}/{min(max_anuncios, len(enlaces))}")
                    moto_data = self.extract_anuncio_data_enhanced(enlace)
                    
                    if moto_data and self.validate_moto_data(moto_data):
                        self.results.append(moto_data)
                        self.processed_urls.add(enlace)
                        anuncios_procesados += 1
                        self.logger.debug(f"Moto válida: {moto_data.get('Título', 'Sin título')[:30]}")
                    else:
                        self.logger.debug(f"Moto no válida o datos incompletos")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.warning(f"Error extrayendo anuncio {i}: {e}")
                    continue
            
            self.logger.info(f"URL procesada: {anuncios_procesados} motos válidas de {len(enlaces)} enlaces")
                    
        except Exception as e:
            self.logger.error(f"Error procesando URL de búsqueda: {e}")
    
    def get_anuncio_links_enhanced(self) -> List[str]:
        """Obtener enlaces con selectores mejorados y más scroll"""
        enlaces = []
        
        try:
            # SCROLL MAS EXTENSIVO
            self.scroll_to_load_more_content()
            
            # SELECTORES ACTUALIZADOS Y MAS COMPRENSIVOS
            link_selectors = [
                "a[href*='/item/']",
                "a[href*='/product/']",
                ".card-product-info a",
                ".item-card a",
                "[data-testid='item-card'] a",
                "a[data-testid*='item']",
                ".product-card a",
                "[class*='Card'] a",
                "[class*='Item'] a"
            ]
            
            for selector in link_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        href = element.get_attribute('href')
                        if href and ('/item/' in href or '/product/' in href) and href not in enlaces:
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
            self.logger.error(f"Error obteniendo enlaces: {e}")
            return []
    
    def scroll_to_load_more_content(self):
        """Scroll más extensivo para cargar más contenido"""
        try:
            # MAS SCROLLS PARA CARGAR MAS CONTENIDO
            max_scrolls = 8 if is_github_actions() else 12
            
            for attempt in range(max_scrolls):
                current_height = self.driver.execute_script("return document.body.scrollHeight")
                
                # Scroll gradual
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.8);")
                time.sleep(1.5)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == current_height:
                    break
            
            # Volver arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
        except Exception as e:
            self.logger.warning(f"Error durante scroll: {e}")
    
    def extract_anuncio_data_enhanced(self, url: str) -> Optional[Dict]:
        """EXTRACCION MEJORADA CON SELECTORES ACTUALIZADOS"""
        try:
            # Cargar página del anuncio
            self.driver.get(url)
            time.sleep(2.5)
            
            # EXTRAER DATOS CON SELECTORES CORREGIDOS
            data = {
                'URL': url,
                'Título': self.extract_titulo_wallapop_fixed(),           
                'Precio': self.extract_precio_wallapop_robust(),          
                'Kilometraje': self.extract_kilometraje_wallapop_fixed(), 
                'Año': self.extract_año_wallapop_fixed(),                 
                'Vendedor': self.extract_vendedor_wallapop_fixed(),       
                'Ubicación': self.extract_ubicacion_wallapop_fixed(),     
                'Fecha_Publicacion': self.extract_fecha_publicacion_wallapop_fixed(),
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            self.logger.warning(f"Error extrayendo datos de {url}: {e}")
            return None
    
    def extract_titulo_wallapop_fixed(self) -> str:
        """TITULO CORREGIDO - Selectores actualizados"""
        try:
            # Esperar a que cargue
            WebDriverWait(self.driver, 3).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            # SELECTORES ACTUALIZADOS PARA TITULO
            selectors = [
                # Selectores principales más actuales
                "h1[data-testid='item-title']",
                "h1.item-detail_title",
                "h1[class*='title']",
                "h1[class*='Title']",
                "h1[class*='ItemDetail']",
                # Fallbacks
                "h1",
                ".title h1",
                "[data-testid*='title'] h1",
                ".item-detail h1",
                ".product-title"
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        title = element.text.strip()
                        if len(title) > 5:  # Filtrar títulos muy cortos
                            return title
                except:
                    continue
            
            # BUSQUEDA EN TODO EL HTML COMO ULTIMO RECURSO
            try:
                h1_elements = self.driver.find_elements(By.TAG_NAME, "h1")
                for h1 in h1_elements:
                    if h1.text.strip() and len(h1.text.strip()) > 5:
                        return h1.text.strip()
            except:
                pass
            
            return "Sin título"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo título: {e}")
            return "Sin título"
    
    def extract_kilometraje_wallapop_fixed(self) -> str:
        """KILOMETRAJE CORREGIDO - Búsqueda mejorada"""
        try:
            # Obtener todo el contenido de la página
            page_source = self.driver.page_source.lower()
            
            # También buscar en elementos específicos
            detail_elements = []
            detail_selectors = [
                ".item-detail",
                ".product-details", 
                ".description",
                "[class*='detail']",
                "[class*='info']",
                "[class*='spec']"
            ]
            
            for selector in detail_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        detail_elements.append(elem.text.lower())
                except:
                    continue
            
            # Combinar todo el texto
            all_text = f"{page_source} {' '.join(detail_elements)}"
            
            # PATRONES MEJORADOS PARA KILOMETRAJE
            km_patterns = [
                # Formatos exactos
                r'(\d{1,3}\.?\d{0,3})\s*km\b',                           # "15.000 km"
                r'(\d{1,3}\.?\d{0,3})\s*kilómetros?\b',                  # "15000 kilómetros"
                r'(\d{1,3}\.?\d{0,3})\s*kms?\b',                         # "15 kms"
                # Con contexto
                r'kilometraje[:\s]*(\d{1,3}\.?\d{0,3})',                 # "kilometraje: 15000"
                r'km[:\s]*(\d{1,3}\.?\d{0,3})',                          # "km: 15000"
                r'recorridos[:\s]*(\d{1,3}\.?\d{0,3})',                  # "recorridos: 15000"
                # Formatos con comas
                r'(\d{1,3}),(\d{3})\s*km',                               # "15,000 km"
                r'(\d{1,3}),(\d{3})\s*kilómetros?',                      # "15,000 kilómetros"
                # Con separadores
                r'(\d{1,3})\s*\.\s*(\d{3})\s*km',                        # "15 . 000 km"
            ]
            
            km_candidates = []
            
            for pattern in km_patterns:
                matches = re.finditer(pattern, all_text)
                for match in matches:
                    try:
                        if len(match.groups()) == 2:  # Formato con separador
                            km_value = int(match.group(1) + match.group(2))
                        else:
                            km_str = match.group(1).replace('.', '').replace(',', '')
                            km_value = int(km_str)
                        
                        # Validar rango razonable para motos
                        if 0 <= km_value <= 200000:
                            km_candidates.append(km_value)
                    except:
                        continue
            
            if km_candidates:
                # Tomar el km más común o el primero válido
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
    
    def extract_año_wallapop_fixed(self) -> str:
        """AÑO CORREGIDO - Búsqueda mejorada"""
        try:
            # Obtener título y contenido
            titulo = self.extract_titulo_wallapop_fixed()
            page_source = self.driver.page_source
            
            # Combinar textos
            combined_text = f"{titulo} {page_source}"
            
            # PATRONES MEJORADOS PARA AÑO
            year_patterns = [
                # En título (más confiable)
                r'\b(20[1-2][0-9])\b(?!\s*(?:€|euros|km|cc|cv))',         # Año en título
                # Con contexto
                r'año[:\s]*(20[1-2][0-9])',                               # "año: 2020"
                r'modelo[:\s]*(20[1-2][0-9])',                            # "modelo: 2020"
                r'del[:\s]*(20[1-2][0-9])',                               # "del 2020"
                r'matriculad[ao][:\s]*(20[1-2][0-9])',                    # "matriculada 2020"
                # En descripción
                r'comprada en (20[1-2][0-9])',                            # "comprada en 2020"
                r'fabricado en (20[1-2][0-9])',                           # "fabricado en 2020"
            ]
            
            year_candidates = []
            
            # Buscar primero en el título (más confiable)
            titulo_lower = titulo.lower()
            for pattern in year_patterns:
                matches = re.finditer(pattern, titulo_lower)
                for match in matches:
                    try:
                        year = int(match.group(1))
                        if 2010 <= year <= 2025:
                            year_candidates.append((year, 10))  # Peso alto para título
                    except:
                        continue
            
            # Buscar en el contenido general
            content_lower = page_source.lower()
            for pattern in year_patterns:
                matches = re.finditer(pattern, content_lower)
                for match in matches:
                    try:
                        year = int(match.group(1))
                        if 2010 <= year <= 2025:
                            year_candidates.append((year, 1))  # Peso normal
                    except:
                        continue
            
            if year_candidates:
                # Ordenar por peso y tomar el de mayor peso
                year_candidates.sort(key=lambda x: x[1], reverse=True)
                return str(year_candidates[0][0])
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo año: {e}")
            return "No especificado"
    
    def extract_vendedor_wallapop_fixed(self) -> str:
        """VENDEDOR CORREGIDO - Detecta comerciales mejor"""
        try:
            # SELECTORES ACTUALIZADOS PARA VENDEDOR
            selectors = [
                # Selectores principales
                "h3[data-testid='seller-name']",
                "h3.seller-name",
                "h3[class*='seller']",
                "h3[class*='user']",
                # Fallbacks
                ".seller-info h3",
                ".user-info h3", 
                ".profile h3",
                "h3"
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
            
            # DETECCION MEJORADA DE COMERCIALES
            vendedor_lower = vendedor_text.lower()
            
            # Palabras clave comerciales más específicas
            commercial_keywords = [
                # Formas jurídicas
                's.l.', 'sl', 's.a.', 'sa', 's.l.u.', 'slu',
                'sociedad', 'empresa', 'compañia', 'cia',
                # Tipos de negocio
                'concesionario', 'taller', 'motor', 'moto', 'auto',
                'dealership', 'dealer', 'garage', 'workshop',
                # Marcas conocidas
                'honda', 'yamaha', 'kawasaki', 'suzuki', 'bmw',
                'ktm', 'ducati', 'triumph', 'harley',
                # Comerciales específicos
                'mundimoto', 'motocity', 'motocard', 'motoplanet',
                'bergmann', 'voge', 'rieju', 'gasgas',
                # Palabras comerciales
                'venta', 'ventas', 'comercial', 'importador',
                'distribuidor', 'mayorista', 'tienda', 'shop'
            ]
            
            # Buscar patrones comerciales
            for keyword in commercial_keywords:
                if keyword in vendedor_lower:
                    return f"Comercial: {vendedor_text}"
            
            # Detectar patrones adicionales
            commercial_patterns = [
                r'\bS\.?L\.?\b',              # S.L.
                r'\bS\.?A\.?\b',              # S.A.
                r'\bLtd\.?\b',                # Ltd
                r'\b\d{3}[\-\s]*\d{3}[\-\s]*\d{3}\b',  # Teléfonos
                r'@',                         # Emails
                r'www\.',                     # Webs
            ]
            
            for pattern in commercial_patterns:
                if re.search(pattern, vendedor_text):
                    return f"Comercial: {vendedor_text}"
            
            return vendedor_text
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo vendedor: {e}")
            return "Particular"
    
    def extract_ubicacion_wallapop_fixed(self) -> str:
        """UBICACION CORREGIDA"""
        try:
            selectors = [
                "a[data-testid='location']",
                "a[class*='location']",
                "a[class*='Location']",
                ".location a",
                ".item-location a",
                "[class*='location'] a"
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        ubicacion = element.text.strip()
                        # Limpiar prefijos
                        ubicacion = ubicacion.replace("en ", "").replace("En ", "")
                        return ubicacion
                except:
                    continue
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo ubicación: {e}")
            return "No especificado"
    
    def extract_fecha_publicacion_wallapop_fixed(self) -> str:
        """FECHA CORREGIDA"""
        try:
            selectors = [
                "span[data-testid='publication-date']",
                "span[class*='date']",
                "span[class*='time']",
                ".publication-date",
                ".item-date"
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
            self.logger.debug(f"Error extrayendo fecha: {e}")
            return "No especificado"
    
    def extract_precio_wallapop_robust(self) -> str:
        """FUNCION YA CORREGIDA - Mantener como está"""
        try:
            # ESPERAR A QUE CARGUEN LOS PRECIOS
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
                )
            except:
                pass
            
            # ESTRATEGIA 1: SELECTORES ESPECIFICOS
            price_selectors = [
                "span.item-detail-price_ItemDetailPrice--standardFinanced__f9ceG",
                ".item-detail-price_ItemDetailPrice--standardFinanced__f9ceG", 
                "span.item-detail-price_ItemDetailPrice--standard__fMa16",
                "span.item-detail-price_ItemDetailPrice--financed__LgMRH",
                ".item-detail-price_ItemDetailPrice--financed__LgMRH",
                "[class*='ItemDetailPrice']",
                "[class*='standardFinanced'] span",
                "[class*='financed'] span"
            ]
            
            for selector in price_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        if text and '€' in text:
                            price = self._extract_price_from_text_wallapop(text)
                            if price != "No especificado":
                                return price
                except:
                    continue
            
            # ESTRATEGIA 2: BUSCAR CUALQUIER PRECIO
            try:
                price_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
                
                valid_prices = []
                for elem in price_elements[:10]:
                    try:
                        text = elem.text.strip().replace('&nbsp;', ' ').replace('\xa0', ' ')
                        if not text:
                            continue
                        
                        price_patterns = [
                            r'(\d{1,3}(?:\.\d{3})+)\s*€',
                            r'(\d{1,6})\s*€'
                        ]
                        
                        for pattern in price_patterns:
                            price_matches = re.findall(pattern, text)
                            for price_match in price_matches:
                                try:
                                    price_clean = price_match.replace('.', '')
                                    price_value = int(price_clean)
                                    
                                    if 500 <= price_value <= 60000:
                                        formatted_price = f"{price_value:,}".replace(',', '.') + " €" if price_value >= 1000 else f"{price_value} €"
                                        valid_prices.append((price_value, formatted_price))
                                except:
                                    continue
                    except:
                        continue
                
                if valid_prices:
                    valid_prices = sorted(set(valid_prices), key=lambda x: x[0], reverse=True)
                    return valid_prices[0][1]
                        
            except:
                pass
            
            return "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo precio: {e}")
            return "No especificado"
    
    def _extract_price_from_text_wallapop(self, text: str) -> str:
        """Extraer precio de texto - YA FUNCIONA"""
        if not text:
            return "No especificado"
        
        clean_text = text.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
        if not clean_text:
            return "No especificado"
        
        price_patterns = [
            r'(\d{1,3}(?:\.\d{3})+)\s*€',           # "7.690 €"
            r'(\d{4,6})\s*€',                       # "7690 €"
            r'(\d{1,2})\s*\.\s*(\d{3})\s*€',        # "7 . 690 €"
            r'(\d{1,2}),(\d{3})\s*€',               # "7,690 €"
            r'€\s*(\d{1,2}\.?\d{3,6})',             # "€ 7690"
            r'(\d{1,2}\.?\d{3,6})\s*euros?',        # "7690 euros"
        ]
        
        for pattern in price_patterns:
            matches = re.finditer(pattern, clean_text, re.IGNORECASE)
            for match in matches:
                try:
                    if len(match.groups()) == 2:  # Formato como 7.690
                        price_value = int(match.group(1) + match.group(2))
                    else:
                        price_str = match.group(1).replace('.', '').replace(',', '')
                        price_value = int(price_str)
                    
                    if 500 <= price_value <= 60000:
                        return f"{price_value:,} €".replace(',', '.')
                except:
                    continue
        
        return "No especificado"
    
    def is_valid_anuncio_link(self, url: str) -> bool:
        """Validar si un enlace es válido para procesar"""
        if not url or ('/item/' not in url and '/product/' not in url):
            return False
        
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
    
    @abstractmethod
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda específicas del modelo"""
        pass
    
    @abstractmethod
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos de la moto corresponden al modelo específico"""
        pass

def test_base_scraper():
    """Función de prueba para la funcionalidad base"""
    print("Probando funcionalidad base del scraper...")
    
    try:
        class TestScraper(BaseScraper):
            def get_search_urls(self):
                return ["https://es.wallapop.com/app/search?keywords=honda%20cb125r"]
            
            def validate_moto_data(self, moto_data):
                return True
        
        scraper = TestScraper('cb125r')
        driver = scraper.setup_driver()
        
        print("Driver configurado correctamente")
        driver.quit()
        print("Driver cerrado correctamente")
        
        return True
        
    except Exception as e:
        print(f"Error en prueba: {e}")
        return False

if __name__ == "__main__":
    test_base_scraper()
