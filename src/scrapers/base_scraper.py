"""
================================================================================
                                BASE SCRAPER                      
================================================================================

Autor: Carlos Peraza
Version: 3.0 
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
from selenium.webdriver.common.action_chains import ActionChains
from typing import Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from tqdm import tqdm

from config import get_modelo_config, SELENIUM_CONFIG, is_github_actions

class BaseScraper(ABC):
    """Clase base ULTRA OPTIMIZADA para scrapers de modelos especificos de motos"""
    
    def __init__(self, modelo_key: str):
        """Inicializar scraper base"""
        self.modelo_key = modelo_key
        self.modelo_config = get_modelo_config(modelo_key)
        self.selenium_config = SELENIUM_CONFIG
        self.logger = logging.getLogger(__name__)
        
        self.driver = None
        self.results = []
        self.processed_urls = set()
        
        # CONTADORES PARA MONITORING
        self.total_processed = 0
        self.successful_extractions = 0
        self.failed_extractions = 0
        self.examples_shown = 0
        
        self.logger.info(f"Scraper ULTRA OPTIMIZADO iniciado para {self.modelo_config['nombre']}")
    
    def setup_driver(self) -> webdriver.Chrome:
        """Configurar driver Chrome ULTRA RAPIDO"""
        options = Options()
        
        # CONFIGURACIONES DE MAXIMA VELOCIDAD
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
        options.add_argument("--disable-client-side-phishing-detection")
        options.add_argument("--disable-sync")
        options.add_argument("--disable-translate")
        options.add_argument("--hide-scrollbars")
        options.add_argument("--mute-audio")
        
        # SUPRIMIR LOGS COMPLETAMENTE
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # User agent optimizado
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Configuracion para GitHub Actions
        if is_github_actions():
            options.add_argument("--headless")
        
        # Configuraciones de rendimiento MAXIMAS
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
            
            # TIMEOUTS ULTRA AGRESIVOS
            driver.implicitly_wait(0.3)  # REDUCIDO de 3.0 a 0.3
            driver.set_page_load_timeout(15)  # REDUCIDO de 45 a 15
            driver.set_script_timeout(10)  # REDUCIDO de 30 a 10
            
            if not is_github_actions():
                driver.maximize_window()
            
            self.logger.info("Driver ULTRA OPTIMIZADO configurado correctamente")
            return driver
            
        except Exception as e:
            self.logger.error(f"Error configurando driver: {e}")
            raise
    
    def scrape_model(self) -> pd.DataFrame:
        """Metodo principal ULTRA OPTIMIZADO para hacer scraping"""
        try:
            self.logger.info(f"Iniciando scraping ULTRA OPTIMIZADO de {self.modelo_config['nombre']}")
            
            # Configurar driver
            self.driver = self.setup_driver()
            
            # Obtener URLs de busqueda
            search_urls = self.get_search_urls()
            self.logger.info(f"{len(search_urls)} URLs de busqueda generadas")
            
            # TIMEOUTS OPTIMIZADOS PARA VELOCIDAD
            start_time = time.time()
            max_total_time = 14400 if is_github_actions() else 18000  # 4-5 horas
            max_urls_to_process = 50 if is_github_actions() else 100  # LIMITAR URLS PARA VELOCIDAD
            
            # PROCESAR SOLO UN SUBCONJUNTO DE URLS PARA VELOCIDAD
            urls_to_process = search_urls[:max_urls_to_process]
            self.logger.info(f"OPTIMIZACION: Procesando {len(urls_to_process)} URLs de {len(search_urls)} totales")
            
            for i, url in enumerate(urls_to_process, 1):
                # Verificar tiempo total
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    self.logger.warning(f"Tiempo limite alcanzado ({max_total_time/3600:.1f} horas)")
                    break
                
                self.logger.info(f"[{i}/{len(urls_to_process)}] Procesando URL: {url[:80]}...")
                
                try:
                    motos_esta_url = self.process_search_url_ultra_optimized(url, i)
                    self.logger.info(f"[{i}/{len(urls_to_process)}] RESULTADO: {motos_esta_url} motos extraidas")
                    
                    # MOSTRAR PROGRESO CADA 10 URLs
                    if i % 10 == 0:
                        self._show_progress_report()
                    
                    time.sleep(1)  # REDUCIDO de 3 a 1 segundo entre URLs
                    
                except Exception as e:
                    self.logger.warning(f"Error procesando URL {i}: {e}")
                    continue
            
            # Convertir resultados a DataFrame
            if self.results:
                df = pd.DataFrame(self.results)
                self.logger.info(f"SCRAPING COMPLETADO: {len(df)} motos encontradas")
                
                # MOSTRAR RESUMEN FINAL DETALLADO
                self._show_final_summary()
                
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
    
    def process_search_url_ultra_optimized(self, url: str, url_number: int) -> int:
        """ULTRA OPTIMIZADO: Procesar URL con implementacion rapida del boton 'Cargar mas'"""
        max_url_time = 600 if is_github_actions() else 900  # REDUCIDO: 10-15 min por URL
        start_url_time = time.time()
        motos_esta_url = 0
        
        try:
            # NAVEGACION ULTRA RAPIDA
            self.driver.get(url)
            time.sleep(2)  # REDUCIDO de 5 a 2
            
            # ACCEPT COOKIES RAPIDO
            self._accept_cookies_fast()
            
            # IMPLEMENTAR CARGA INTELIGENTE DE ANUNCIOS (del scraper MOTICK)
            total_enlaces = self._smart_load_all_ads_optimized()
            
            if total_enlaces == 0:
                self.logger.warning(f"[URL {url_number}] No se encontraron enlaces")
                return 0
            
            # OBTENER ENLACES FINALES
            enlaces = self._get_anuncio_links_ultra_fast()
            
            if not enlaces:
                self.logger.warning(f"[URL {url_number}] No se encontraron enlaces procesables")
                return 0
            
            self.logger.info(f"[URL {url_number}] Procesando {len(enlaces)} enlaces encontrados")
            
            # PROCESAR ANUNCIOS CON BARRA DE PROGRESO
            motos_esta_url = self._process_anuncios_ultra_fast(enlaces, url_number, start_url_time, max_url_time)
            
            return motos_esta_url
                    
        except Exception as e:
            self.logger.error(f"Error procesando URL de busqueda: {e}")
            return 0
    
    def _accept_cookies_fast(self):
        """Acepta cookies de forma ultra rapida"""
        try:
            cookie_button = WebDriverWait(self.driver, 2).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            cookie_button.click()
            time.sleep(0.3)
        except:
            pass
    
    def _smart_load_all_ads_optimized(self) -> int:
        """Carga todos los anuncios de forma inteligente - BASADO EN SCRAPER MOTICK"""
        expected_count = 50  # OBJETIVO REALISTA
        max_clicks = 8  # REDUCIDO de 20 a 8
        
        # Scroll inicial ultra rapido
        for i in range(2):
            self.driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(0.2)
        
        initial_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        
        clicks_realizados = 0
        last_count = initial_count
        
        for click_num in range(max_clicks):
            # Scroll rapido hasta abajo
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            
            if self._find_and_click_load_more_fast():
                clicks_realizados += 1
                
                # Espera MINIMA para carga
                time.sleep(1)  # REDUCIDO de 4 a 1
                
                new_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
                
                if new_count > last_count:
                    self.logger.info(f"[SMART] Clic {clicks_realizados}: {last_count} -> {new_count} (+{new_count - last_count})")
                    last_count = new_count
                    
                    if new_count >= expected_count:
                        self.logger.info(f"[SMART] Objetivo alcanzado: {new_count} anuncios")
                        break
                else:
                    self.logger.info(f"[SMART] Sin nuevos anuncios, fin del contenido")
                    break
            else:
                self.logger.info(f"[SMART] Boton no encontrado, fin del contenido")
                break
        
        final_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        self.logger.info(f"[SMART] Total final: {final_count} anuncios ({clicks_realizados} clics)")
        
        return final_count
    
    def _find_and_click_load_more_fast(self) -> bool:
        """Busca y hace clic en 'Cargar mas' - OPTIMIZADO del scraper MOTICK"""
        
        # SELECTORES OPTIMIZADOS del scraper MOTICK
        selectors = [
            ('css', 'walla-button[text="Cargar más"]'),
            ('css', 'walla-button[text*="Cargar"]'),
            ('css', 'button.walla-button__button'),
            ('css', '.walla-button__button'),
            ('xpath', '//span[text()="Cargar más"]/ancestor::button'),
            ('xpath', '//span[contains(text(), "Cargar")]/ancestor::button'),
            ('xpath', '//button[contains(@class, "walla-button")]'),
            ('css', '[class*="load-more"]')
        ]
        
        for selector_type, selector in selectors:
            try:
                if selector_type == 'css':
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                else:
                    elements = self.driver.find_elements(By.XPATH, selector)
                
                for element in elements:
                    try:
                        if not element.is_displayed() or not element.is_enabled():
                            continue
                        
                        # Scroll y clic ULTRA RAPIDO
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        time.sleep(0.1)  # MINIMO
                        
                        try:
                            element.click()
                            self.logger.info(f"Clic exitoso en 'Cargar mas' con selector: {selector_type}:{selector}")
                            time.sleep(0.3)
                            return True
                        except:
                            try:
                                self.driver.execute_script("arguments[0].click();", element)
                                self.logger.info(f"Clic con JS exitoso en 'Cargar mas'")
                                time.sleep(0.3)
                                return True
                            except:
                                continue
                    except:
                        continue
            except:
                continue
        
        return False
    
    def _get_anuncio_links_ultra_fast(self) -> List[str]:
        """ULTRA RAPIDO: Obtener enlaces con selectores optimizados"""
        enlaces = []
        
        try:
            # SELECTORES ULTRA RAPIDOS
            link_selectors = [
                'a[href*="/item/"]',
                'a[href*="/app/user/"]'
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
            
            return list(set(enlaces))  # Eliminar duplicados
            
        except Exception as e:
            self.logger.error(f"Error obteniendo enlaces: {e}")
            return []
    
    def _process_anuncios_ultra_fast(self, enlaces: List[str], url_number: int, start_url_time: float, max_url_time: float) -> int:
        """Procesar anuncios con VELOCIDAD MAXIMA y logging detallado"""
        anuncios_procesados = 0
        max_anuncios_por_url = 30 if is_github_actions() else 50  # LIMITE REALISTA
        
        # CONTADORES PARA MONITORING DETALLADO
        precios_ok = 0
        km_ok = 0
        titles_ok = 0
        
        # USAR TQDM PARA BARRA DE PROGRESO
        enlaces_a_procesar = enlaces[:max_anuncios_por_url]
        
        for i, enlace in enumerate(tqdm(enlaces_a_procesar, desc=f"URL {url_number}", colour="green"), 1):
            # Verificar timeout
            elapsed_url_time = time.time() - start_url_time
            if elapsed_url_time > max_url_time:
                self.logger.warning(f"Timeout alcanzado en URL {url_number}")
                break
            
            if enlace in self.processed_urls:
                continue
            
            try:
                # NAVEGACION ULTRA RAPIDA
                self.driver.get(enlace)
                time.sleep(0.5)  # MINIMO DELAY
                
                # EXTRACCION ULTRA RAPIDA
                moto_data = self._extract_anuncio_data_ultra_fast(enlace)
                
                if moto_data and self.validate_moto_data(moto_data):
                    self.results.append(moto_data)
                    self.processed_urls.add(enlace)
                    anuncios_procesados += 1
                    self.total_processed += 1
                    
                    # CONTEO PARA MONITORING
                    if moto_data.get('Precio') != "No especificado":
                        precios_ok += 1
                    if moto_data.get('Kilometraje') != "No especificado":
                        km_ok += 1
                    if moto_data.get('Titulo') != "Sin titulo":
                        titles_ok += 1
                    
                    # MOSTRAR EJEMPLOS DE LOS PRIMEROS 3 ANUNCIOS
                    if self.examples_shown < 3:
                        titulo = moto_data.get('Titulo', 'N/A')[:30]
                        precio = moto_data.get('Precio', 'N/A')
                        km = moto_data.get('Kilometraje', 'N/A')
                        año = moto_data.get('Año', 'N/A')
                        self.logger.info(f"[EJEMPLO {self.examples_shown + 1}] {titulo}... | {precio} | {km} | {año}")
                        self.examples_shown += 1
                
                # SIN DELAY entre anuncios para maxima velocidad
                
            except Exception as e:
                self.failed_extractions += 1
                continue
        
        # RESUMEN POR URL
        if anuncios_procesados > 0:
            precio_pct = (precios_ok / anuncios_procesados * 100) if anuncios_procesados > 0 else 0
            km_pct = (km_ok / anuncios_procesados * 100) if anuncios_procesados > 0 else 0
            titles_pct = (titles_ok / anuncios_procesados * 100) if anuncios_procesados > 0 else 0
            
            self.logger.info(f"[URL {url_number}] RESUMEN: {anuncios_procesados} motos procesadas")
            self.logger.info(f"[URL {url_number}] CALIDAD: Titulos {titles_pct:.1f}% | Precios {precio_pct:.1f}% | KM {km_pct:.1f}%")
        
        return anuncios_procesados
    
    def _extract_anuncio_data_ultra_fast(self, url: str) -> Optional[Dict]:
        """ULTRA RAPIDO: Extraccion de datos optimizada"""
        try:
            # EXTRACCION RAPIDA CON TIMEOUT MINIMO
            data = {
                'URL': url,
                'Titulo': self._extract_titulo_fast(),           
                'Precio': self._extract_precio_fast(),          
                'Kilometraje': self._extract_kilometraje_fast(), 
                'Año': self._extract_año_fast(),                 
                'Vendedor': self._extract_vendedor_fast(),       
                'Ubicacion': self._extract_ubicacion_fast(),     
                'Fecha_Publicacion': self._extract_fecha_publicacion_fast(),
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            return None
    
    def _extract_titulo_fast(self) -> str:
        """ULTRA RAPIDO: Titulo con timeout minimo"""
        try:
            # SELECTORES ULTRA RAPIDOS
            selectors = ['h1', '*[class*="title"] h1', '*[class*="Title"] h1']
            
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
            
        except:
            return "Sin titulo"
    
    def _extract_precio_fast(self) -> str:
        """ULTRA RAPIDO: Precio con selectores del scraper MOTICK exitoso"""
        try:
            # SELECTORES EXITOSOS del scraper MOTICK
            price_selectors = [
                "span.item-detail-price_ItemDetailPrice--standardFinanced__f9ceG",
                ".item-detail-price_ItemDetailPrice--standardFinanced__f9ceG", 
                "span.item-detail-price_ItemDetailPrice--standard__fMa16",
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
                            price = self._extract_price_from_text_fast(text)
                            if price != "No especificado":
                                return price
                except:
                    continue
            
            # BUSQUEDA RAPIDA DE PRECIOS
            try:
                price_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
                for elem in price_elements[:3]:  # SOLO PRIMEROS 3
                    text = elem.text.strip()
                    if text and len(text) < 20:
                        price = self._extract_price_from_text_fast(text)
                        if price != "No especificado":
                            return price
            except:
                pass
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def _extract_price_from_text_fast(self, text: str) -> str:
        """ULTRA RAPIDO: Extraer precio - del scraper MOTICK exitoso"""
        if not text:
            return "No especificado"
        
        clean_text = text.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
        
        # REGEX ULTRA RAPIDOS
        price_patterns = [
            r'(\d{1,3}(?:\.\d{3})+)\s*€',
            r'(\d{4,6})\s*€'
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
    
    def _extract_kilometraje_fast(self) -> str:
        """ULTRA RAPIDO: Kilometraje con busqueda rapida"""
        try:
            # BUSQUEDA RAPIDA EN DESCRIPCION
            try:
                desc_element = self.driver.find_element(By.CSS_SELECTOR, "[class*='description']")
                desc_text = desc_element.text.lower()
                
                # REGEX ULTRA RAPIDO
                km_match = re.search(r'kilómetros:\s*(\d{1,3}(?:\.\d{3})*)', desc_text)
                if km_match:
                    km_value = int(km_match.group(1).replace('.', ''))
                    if 0 <= km_value <= 999999:
                        return f"{km_value:,} km".replace(',', '.')
            except:
                pass
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def _extract_año_fast(self) -> str:
        """ULTRA RAPIDO: Año con busqueda rapida"""
        try:
            # BUSQUEDA EN TITULO
            titulo = self._extract_titulo_fast()
            year_match = re.search(r'\b(20[1-2][0-9])\b', titulo)
            if year_match:
                year = int(year_match.group(1))
                if 2010 <= year <= 2025:
                    return str(year)
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def _extract_vendedor_fast(self) -> str:
        """ULTRA RAPIDO: Vendedor basico"""
        try:
            # BUSQUEDA RAPIDA
            selectors = ['h3', '*[class*="user"] h3', '*[class*="seller"] h3']
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        vendedor = element.text.strip()
                        if 'comercial' in vendedor.lower() or 's.l.' in vendedor.lower():
                            return "Comercial"
                        return "Particular"
                except:
                    continue
            
            return "Particular"
            
        except:
            return "Particular"
    
    def _extract_ubicacion_fast(self) -> str:
        """ULTRA RAPIDO: Ubicacion basica"""
        try:
            selectors = ['*[class*="location"]', '*[data-testid*="location"]']
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        return element.text.strip()
                except:
                    continue
            
            return "No especificado"
            
        except:
            return "No especificado"
    
    def _extract_fecha_publicacion_fast(self) -> str:
        """ULTRA RAPIDO: Fecha basica"""
        return "No especificado"  # SIMPLIFICADO por velocidad
    
    def _show_progress_report(self):
        """Mostrar reporte de progreso detallado"""
        if self.total_processed > 0:
            success_rate = (self.successful_extractions / self.total_processed * 100) if self.total_processed > 0 else 0
            self.logger.info(f"[PROGRESO] {self.total_processed} anuncios procesados | Tasa exito: {success_rate:.1f}%")
    
    def _show_final_summary(self):
        """Mostrar resumen final detallado"""
        self.logger.info("="*60)
        self.logger.info("RESUMEN FINAL SCRAPING")
        self.logger.info("="*60)
        self.logger.info(f"Total anuncios procesados: {self.total_processed}")
        self.logger.info(f"Motos validas extraidas: {len(self.results)}")
        self.logger.info(f"URLs procesadas unicas: {len(self.processed_urls)}")
        
        if self.results:
            df = pd.DataFrame(self.results)
            
            # CALCULAR CALIDADES
            titles_ok = len(df[df['Titulo'] != 'Sin titulo'])
            prices_ok = len(df[df['Precio'] != 'No especificado'])
            km_ok = len(df[df['Kilometraje'] != 'No especificado'])
            
            total = len(df)
            self.logger.info(f"CALIDAD EXTRACCION:")
            self.logger.info(f"  Titulos: {titles_ok}/{total} ({titles_ok/total*100:.1f}%)")
            self.logger.info(f"  Precios: {prices_ok}/{total} ({prices_ok/total*100:.1f}%)")
            self.logger.info(f"  Kilometrajes: {km_ok}/{total} ({km_ok/total*100:.1f}%)")
            
            # MOSTRAR EJEMPLOS FINALES
            self.logger.info(f"EJEMPLOS FINALES:")
            for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
                titulo = row['Titulo'][:30] if len(row['Titulo']) > 30 else row['Titulo']
                self.logger.info(f"  {i}. {titulo} | {row['Precio']} | {row['Kilometraje']} | {row['Año']}")
        
        self.logger.info("="*60)
    
    @abstractmethod
    def get_search_urls(self) -> List[str]:
        """Generar URLs de busqueda especificas del modelo"""
        pass
    
    @abstractmethod
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos de la moto corresponden al modelo especifico"""
        pass
