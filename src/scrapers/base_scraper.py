"""
================================================================================
                    BASE SCRAPER COMPLETO CORREGIDO - WALLAPOP MOTOS SCRAPER                    
================================================================================

Clase base con extracción de precios corregida usando la lógica probada del scraper MOTICK
Código completo, limpio y sin errores de sintaxis

Autor: Carlos Peraza
Versión: 1.2 - Completo y corregido
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
            
            # Configurar timeouts
            driver.implicitly_wait(0.5)
            driver.set_page_load_timeout(10)
            driver.set_script_timeout(8)
            
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
            self.logger.info(f"Iniciando scraping de {self.modelo_config['nombre']}")
            
            # Configurar driver
            self.driver = self.setup_driver()
            
            # Obtener URLs de búsqueda específicas del modelo
            search_urls = self.get_search_urls()
            self.logger.info(f"{len(search_urls)} URLs de búsqueda generadas")
            
            # Procesar cada URL con límite de tiempo
            start_time = time.time()
            max_total_time = 1800 if is_github_actions() else 3600
            
            for i, url in enumerate(search_urls, 1):
                # Verificar tiempo total
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    self.logger.warning(f"Tiempo límite alcanzado ({max_total_time/60:.1f} min)")
                    break
                
                self.logger.info(f"Procesando URL {i}/{len(search_urls)}: {url[:80]}...")
                
                try:
                    self.process_search_url_with_timeout(url)
                    time.sleep(1)
                except Exception as e:
                    self.logger.warning(f"Error procesando URL {i}: {e}")
                    continue
            
            # Convertir resultados a DataFrame
            if self.results:
                df = pd.DataFrame(self.results)
                self.logger.info(f"Scraping completado: {len(df)} motos encontradas")
                
                # DEBUG: Mostrar precios extraídos para diagnóstico
                self._debug_extracted_prices(df)
                
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
    
    def _debug_extracted_prices(self, df: pd.DataFrame):
        """Función debug para mostrar precios extraídos"""
        print("\n=== DEBUG: PRECIOS EXTRAÍDOS ===")
        for i, row in df.head(5).iterrows():
            print(f"{i+1}. Título: {row['Título'][:40]}...")
            print(f"   Precio raw: '{row['Precio']}'")
            print(f"   Precio limpio: '{self._clean_price_for_debug(row['Precio'])}'")
        print("================================\n")
    
    def _clean_price_for_debug(self, price_text: str) -> str:
        """Función debug para mostrar limpieza de precios"""
        if not price_text or price_text == "No especificado":
            return "0"
        
        # Mostrar paso a paso la limpieza
        step1 = price_text.replace('&nbsp;', ' ').replace('\xa0', ' ')
        step2 = re.sub(r'[^\d.,€]', '', step1)
        step3 = re.findall(r'\d+', step2.replace('.', '').replace(',', ''))
        
        print(f"     Debug: '{price_text}' -> '{step1}' -> '{step2}' -> {step3}")
        
        if step3:
            return step3[0]
        return "0"
    
    def process_search_url_with_timeout(self, url: str):
        """Procesar una URL de búsqueda con timeout"""
        max_url_time = 600 if is_github_actions() else 900
        start_url_time = time.time()
        
        try:
            # Cargar página con timeout
            self.driver.get(url)
            time.sleep(2)
            
            # Obtener enlaces de anuncios
            enlaces = self.get_anuncio_links()
            
            if not enlaces:
                self.logger.warning(f"No se encontraron enlaces en esta URL")
                return
            
            self.logger.info(f"{len(enlaces)} enlaces encontrados")
            
            # Procesar cada anuncio con límite
            max_anuncios = 10 if is_github_actions() else len(enlaces)
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
                    moto_data = self.extract_anuncio_data_robust(enlace)
                    
                    if moto_data and self.validate_moto_data(moto_data):
                        self.results.append(moto_data)
                        self.processed_urls.add(enlace)
                        anuncios_procesados += 1
                        self.logger.debug(f"Moto válida: {moto_data.get('Título', 'Sin título')[:30]}")
                    else:
                        self.logger.debug(f"Moto no válida o datos incompletos")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.warning(f"Error extrayendo anuncio {i}: {e}")
                    continue
            
            self.logger.info(f"URL procesada: {anuncios_procesados} motos válidas de {len(enlaces)} enlaces")
                    
        except Exception as e:
            self.logger.error(f"Error procesando URL de búsqueda: {e}")
    
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
            self.logger.error(f"Error obteniendo enlaces: {e}")
            return []
    
    def scroll_to_load_content(self):
        """Hacer scroll optimizado para cargar contenido dinámico"""
        try:
            max_scrolls = 2 if is_github_actions() else 3
            
            for attempt in range(max_scrolls):
                current_height = self.driver.execute_script("return document.body.scrollHeight")
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == current_height:
                    break
            
            # Volver arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
            
        except Exception as e:
            self.logger.warning(f"Error durante scroll: {e}")
    
    def extract_anuncio_data_robust(self, url: str) -> Optional[Dict]:
        """Extraer datos con selectores actualizados de Wallapop"""
        try:
            # Cargar página del anuncio
            self.driver.get(url)
            time.sleep(1.5)
            
            # Extraer datos usando selectores actualizados
            data = {
                'URL': url,
                'Título': self.extract_titulo_wallapop(),
                'Precio': self.extract_precio_wallapop_robust(),  # FUNCIÓN MEJORADA
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
            self.logger.warning(f"Error extrayendo datos de {url}: {e}")
            return None
    
    def extract_titulo_wallapop(self) -> str:
        """Extraer título con selector real de Wallapop"""
        selectors = [
            "h1.item-detail_ItemDetailTwoColumns__title__VtWrR",
            "h1[class*='ItemDetailTwoColumns__title']",
            "h1[class*='title']",
            "h1"
        ]
        
        return self._extract_text_by_selectors(selectors, "Sin título")
    
    def extract_precio_wallapop_robust(self) -> str:
        """FUNCIÓN ADAPTADA: Extraer precio usando la lógica probada del scraper MOTICK"""
        try:
            # ESPERAR A QUE CARGUEN LOS PRECIOS (del scraper exitoso)
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
                )
            except:
                pass
            
            # ESTRATEGIA 1: SELECTORES ESPECÍFICOS (del scraper exitoso)
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
            
            # ESTRATEGIA 2: BUSCAR CUALQUIER PRECIO (método exitoso del scraper MOTICK)
            try:
                price_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
                
                valid_prices = []
                for elem in price_elements[:10]:
                    try:
                        text = elem.text.strip().replace('&nbsp;', ' ').replace('\xa0', ' ')
                        if not text:
                            continue
                        
                        # REGEX PARA CAPTURAR PRECIOS REALISTAS
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
                                    
                                    # RANGO PARA MOTOS: 500€ - 60,000€
                                    if 500 <= price_value <= 60000:
                                        formatted_price = f"{price_value:,}".replace(',', '.') + " €" if price_value >= 1000 else f"{price_value} €"
                                        valid_prices.append((price_value, formatted_price))
                                except:
                                    continue
                    except:
                        continue
                
                # Tomar el precio más alto como precio principal
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
        """Extraer precio de texto - ADAPTADO del scraper MOTICK exitoso"""
        if not text:
            return "No especificado"
        
        # Limpiar texto (como en el scraper exitoso)
        clean_text = text.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
        if not clean_text:
            return "No especificado"
        
        # REGEX ESPECÍFICOS PARA WALLAPOP (del scraper exitoso)
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
                    
                    # RANGO PARA MOTOS: 500€ - 60,000€
                    if 500 <= price_value <= 60000:
                        return f"{price_value:,} €".replace(',', '.')
                except:
                    continue
        
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
        return descripcion[:500] if descripcion else ""
    
    def extract_año_wallapop(self) -> str:
        """Extraer año usando la función robusta"""
        try:
            descripcion = self.extract_descripcion_wallapop()
            titulo = self.extract_titulo_wallapop()
            
            año, _ = self.extract_year_and_km_universal(f"{titulo} {descripcion}")
            return año
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo año: {e}")
            return "No especificado"
    
    def extract_kilometraje_wallapop(self) -> str:
        """Extraer kilometraje usando la función robusta"""
        try:
            descripcion = self.extract_descripcion_wallapop()
            titulo = self.extract_titulo_wallapop()
            
            _, km = self.extract_year_and_km_universal(f"{titulo} {descripcion}")
            return km if km else "No especificado"
            
        except Exception as e:
            self.logger.debug(f"Error extrayendo km: {e}")
            return "No especificado"
    
    def extract_year_and_km_universal(self, text: str) -> Tuple[str, str]:
        """Función robusta de extracción adaptada"""
        if not text:
            return "No especificado", "No especificado"
        
        text_normalized = text.lower().replace('\n', ' ').replace('\t', ' ')
        text_normalized = re.sub(r'\s+', ' ', text_normalized)
        
        # EXTRACCIÓN DE AÑO
        year = "No especificado"
        all_year_candidates = []
        
        year_patterns = [
            (r'año\s*[:\-]\s*(201[3-9]|202[0-4])', 22, "año: directo"),
            (r'año\s+(201[3-9]|202[0-4])', 20, "año XXXX"),
            (r'del\s+año\s+(201[3-9]|202[0-4])', 20, "del año XXXX"),
            (r'(?:modelo|model)\s+(?:del\s+)?(?:año\s+)?(201[3-9]|202[0-4])', 20, "modelo del año"),
            (r'matriculad[ao]\s+(?:en\s+(?:el\s+año\s+)?)?(?:en\s+)?(201[3-9]|202[0-4])', 18, "matriculada"),
            (r'(?:honda|yamaha|kawasaki|kymco|cb|mt|z|pcx|agility)\s+(?:del\s+año\s+|año\s+|de\s+)?(201[3-9]|202[0-4])', 18, "marca año"),
            (r'del\s+(201[3-9]|202[0-4])(?!\s*(?:€|euros|km|kms|klm))(?:\s|$|\.|\,)', 14, "del XXXX"),
            (r'\b(201[3-9]|202[0-4])\b(?!\s*(?:€|euros|km|kms|klm|cc|cv|hp|precio))', 8, "año standalone"),
        ]
        
        exclude_patterns = [
            r'(?:itv|itvs)\s+(?:hasta|vigente|válida|pasada|en|del|año|de)\s+(201[3-9]|202[0-4])',
            r'revisión\s+(?:en|del|año|hasta|de)\s+(201[3-9]|202[0-4])',
            r'(201[3-9]|202[0-4])€',
            r'€(201[3-9]|202[0-4])',
            r'(201[3-9]|202[0-4])cc',
        ]
        
        for pattern, score, description in year_patterns:
            matches = re.finditer(pattern, text_normalized)
            for match in matches:
                found_year = match.group(1)
                
                try:
                    year_value = int(found_year)
                    if 2013 <= year_value <= 2024:
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
        
        if all_year_candidates:
            year = min(candidate['year'] for candidate in all_year_candidates)
        
        # EXTRACCIÓN DE KILÓMETROS
        km = "No especificado"
        all_km_candidates = []
        
        km_patterns = [
            r'km\s*[:\-]\s*(\d{1,3})\.(\d{3})',
            r'kilómetros?\s*[:\-]\s*(\d{1,3})\.(\d{3})',
            r'km\s*[:\-]\s*(\d+)',
            r'(\d{1,3})\.(\d{3})\s*km',
            r'(\d+)\s*km',
            r'(?:solo|tiene|lleva|marca)\s+(\d{1,3})\.(\d{3})',
            r'(?:solo|tiene|lleva|marca)\s+(\d+)',
            r'(\d+)\s*mil\s*km',
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
                            km_value = int(groups[0] + groups[1])
                    
                    if 0 <= km_value <= 200000:
                        all_km_candidates.append({
                            'value': km_value,
                            'context': match.group(0)
                        })
                        
                except (ValueError, TypeError):
                    continue
        
        if all_km_candidates:
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
                element = WebDriverWait(self.driver, 1).until(
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
