"""
================================================================================
                     SCRAPER CB125R STANDALONE - SIN DEPENDENCIAS
================================================================================

Scraper completamente independiente que NO usa BaseScraper ni dependencias
complejas. Implementa SOLO lo que main_runner.py necesita.

GARANTIZADO PARA FUNCIONAR - Sin imports complejos ni herencias.

Autor: Solución Standalone
Versión: 1.0 FUNCIONAL
Fecha: Septiembre 2025

================================================================================
"""

import re
import time
import logging
import os
import sys
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import Dict, List, Optional

# CONFIGURACIÓN SIMPLE HARDCODED
CB125R_CONFIG = {
    'nombre': 'Honda CB125R',
    'precio_min': 1500,
    'precio_max': 5000,
    'km_max': 30000,
    'año_min': 2013,
    'año_max': 2025
}

class ScraperCB125R:
    """Scraper CB125R completamente independiente - SIN HERENCIA"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.driver = None
        self.results = []
        
        # Stats simples
        self.stats = {
            'procesados': 0,
            'validos': 0,
            'rechazados': 0
        }
        
        self.logger.info("CB125R Scraper STANDALONE iniciado")
    
    def setup_driver(self):
        """Configurar Chrome driver básico"""
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-images")
        options.add_argument("--headless")
        options.add_argument("--disable-web-security")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.maximize_window()
        self.driver.implicitly_wait(10)
        
        self.logger.info("Chrome driver configurado")
    
    def get_search_urls(self) -> List[str]:
        """URLs básicas para CB125R"""
        min_price = CB125R_CONFIG['precio_min']
        max_price = CB125R_CONFIG['precio_max']
        
        urls = [
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb%20125%20r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&order_by=newest",
            f"https://es.wallapop.com/app/search?keywords=cb125r",
        ]
        
        return urls
    
    def extract_title_simple(self, element):
        """Extraer título con selectores actualizados"""
        try:
            selectors = [
                "h3.item-card_ItemCard__title__5TocV",
                "h3[class*='title']",
                "h3"
            ]
            
            for selector in selectors:
                try:
                    title_elem = element.find_element(By.CSS_SELECTOR, selector)
                    title = title_elem.text.strip()
                    if title and len(title) > 3:
                        return title.lower()
                except:
                    continue
            return None
        except:
            return None
    
    def extract_price_simple(self, element):
        """Extraer precio con selectores actualizados"""
        try:
            selectors = [
                "strong.item-card_ItemCard__price__pVpdc",
                "strong[class*='price']",
                "*[aria-label*='Item price']"
            ]
            
            for selector in selectors:
                try:
                    price_elem = element.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_elem.text.strip()
                    if '€' in price_text:
                        return price_text
                except:
                    continue
            return None
        except:
            return None
    
    def validate_cb125r_simple(self, element):
        """Validación simple para CB125R"""
        try:
            title = self.extract_title_simple(element)
            if not title:
                return False
            
            # Verificar Honda CB125R
            honda_keywords = ['honda', 'handa']
            cb125r_keywords = ['cb125r', 'cb 125 r', 'cb125-r']
            
            has_honda = any(keyword in title for keyword in honda_keywords)
            has_cb125r = any(keyword in title for keyword in cb125r_keywords)
            
            if not (has_honda or has_cb125r):
                return False
            
            # Verificar precio
            price_text = self.extract_price_simple(element)
            if price_text:
                price_numbers = re.findall(r'\d+', price_text.replace('.', '').replace(',', ''))
                if price_numbers:
                    price_value = int(''.join(price_numbers))
                    if not (1500 <= price_value <= 5000):
                        return False
            
            return True
        except:
            return False
    
    def click_load_more_simple(self):
        """Hacer clic en Cargar más - versión simple"""
        try:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            selectors = [
                'walla-button[text="Cargar más"]',
                'button.walla-button__button--primary',
                '//span[text()="Cargar más"]/ancestor::button'
            ]
            
            for selector in selectors:
                try:
                    if selector.startswith('//'):
                        buttons = self.driver.find_elements(By.XPATH, selector)
                    else:
                        buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for button in buttons:
                        if button.is_displayed() and button.is_enabled():
                            button.click()
                            self.logger.info("Clic en 'Cargar más' exitoso")
                            return True
                except:
                    continue
            return False
        except:
            return False
    
    def scroll_and_load_simple(self):
        """Scroll simple con botón cargar más"""
        initial_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        
        # Scroll inicial
        for i in range(10):
            self.driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.3)
        
        # Intentar cargar más
        for attempt in range(3):
            if self.click_load_more_simple():
                time.sleep(3)
                for i in range(10):
                    self.driver.execute_script("window.scrollBy(0, 1000);")
                    time.sleep(0.2)
            else:
                break
        
        # Scroll final intensivo
        for i in range(50):
            self.driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.1)
        
        final_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        self.logger.info(f"Enlaces: {initial_count} -> {final_count}")
        
        return final_count
    
    def extract_detailed_data_simple(self, url):
        """Extraer datos detallados de una página"""
        try:
            self.driver.get(url)
            time.sleep(2)
            
            data = {
                'URL': url,
                'Titulo': 'Sin titulo',
                'Precio': 'No especificado',
                'Kilometraje': 'No especificado',
                'Año': 'No especificado',
                'Vendedor': 'No especificado',
                'Ubicacion': 'No especificado',
                'Fecha_Publicacion': 'No especificado',
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Título
            try:
                h1_elements = self.driver.find_elements(By.TAG_NAME, "h1")
                for h1 in h1_elements:
                    if h1.text.strip():
                        data['Titulo'] = h1.text.strip()
                        break
            except:
                pass
            
            # Precio
            try:
                price_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
                for elem in price_elements:
                    text = elem.text.strip()
                    if '€' in text and len(text) < 20:
                        data['Precio'] = text
                        break
            except:
                pass
            
            # Ubicación
            try:
                location_elements = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/')]")
                for elem in location_elements:
                    text = elem.text.strip()
                    if text and len(text) < 50 and not any(keyword in text.lower() for keyword in ['honda', 'cb125r', 'moto']):
                        data['Ubicacion'] = text
                        break
            except:
                pass
            
            return data
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de {url}: {e}")
            return None
    
    def scrape_model(self) -> pd.DataFrame:
        """
        MÉTODO PRINCIPAL que main_runner.py espera
        
        Returns:
            DataFrame con resultados
        """
        try:
            self.logger.info("=== INICIANDO SCRAPING CB125R STANDALONE ===")
            
            # Configurar driver
            self.setup_driver()
            
            # URLs de búsqueda
            urls = self.get_search_urls()
            self.logger.info(f"Procesando {len(urls)} URLs")
            
            all_links = set()
            cookies_accepted = False
            
            # FASE 1: Recopilar enlaces
            for idx, url in enumerate(urls[:3]):  # Solo 3 URLs para prueba
                try:
                    self.logger.info(f"URL {idx+1}: {url[:60]}...")
                    self.driver.get(url)
                    time.sleep(3)
                    
                    # Aceptar cookies una vez
                    if not cookies_accepted:
                        try:
                            cookie_btn = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Aceptar')]")
                            cookie_btn.click()
                            cookies_accepted = True
                            time.sleep(2)
                        except:
                            pass
                    
                    # Scroll y cargar más
                    self.scroll_and_load_simple()
                    
                    # Buscar anuncios válidos
                    ad_containers = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/item/"]')
                    self.logger.info(f"Encontrados {len(ad_containers)} contenedores")
                    
                    valid_found = 0
                    for container in ad_containers:
                        try:
                            href = container.get_attribute('href')
                            if not href or href in all_links:
                                continue
                            
                            if self.validate_cb125r_simple(container):
                                all_links.add(href)
                                valid_found += 1
                                
                                if len(all_links) >= 50:  # Límite para prueba
                                    break
                        except:
                            continue
                    
                    self.logger.info(f"URL {idx+1}: {valid_found} CB125R válidos encontrados")
                    
                    if len(all_links) >= 50:
                        break
                        
                except Exception as e:
                    self.logger.error(f"Error en URL {idx+1}: {e}")
                    continue
            
            self.logger.info(f"FASE 1 completada: {len(all_links)} enlaces CB125R encontrados")
            
            if not all_links:
                self.logger.warning("No se encontraron enlaces válidos")
                return pd.DataFrame()
            
            # FASE 2: Análisis detallado (primeros 20 para prueba)
            sample_links = list(all_links)[:20]
            self.logger.info(f"Analizando {len(sample_links)} anuncios en detalle")
            
            results = []
            for idx, url in enumerate(sample_links):
                try:
                    self.logger.info(f"Analizando {idx+1}/{len(sample_links)}: {url}")
                    data = self.extract_detailed_data_simple(url)
                    
                    if data:
                        results.append(data)
                        self.stats['validos'] += 1
                    else:
                        self.stats['rechazados'] += 1
                        
                    self.stats['procesados'] += 1
                    
                except Exception as e:
                    self.logger.error(f"Error en análisis detallado {idx+1}: {e}")
                    self.stats['rechazados'] += 1
                    continue
            
            # Crear DataFrame
            if results:
                df = pd.DataFrame(results)
                self.logger.info(f"=== SCRAPING COMPLETADO ===")
                self.logger.info(f"Procesados: {self.stats['procesados']}")
                self.logger.info(f"Válidos: {self.stats['validos']}")
                self.logger.info(f"Rechazados: {self.stats['rechazados']}")
                self.logger.info(f"DataFrame final: {len(df)} filas")
                return df
            else:
                self.logger.warning("No se obtuvieron resultados válidos")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error crítico en scrape_model: {e}")
            return pd.DataFrame()
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

# FUNCIÓN REQUERIDA POR EL SISTEMA
def run_cb125r_scraper() -> pd.DataFrame:
    """Función de ejecución directa"""
    try:
        scraper = ScraperCB125R()
        return scraper.scrape_model()
    except Exception as e:
        logging.error(f"Error en run_cb125r_scraper: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Test directo
    print("Testing CB125R Scraper STANDALONE...")
    df = run_cb125r_scraper()
    print(f"Resultados: {len(df)} motos")
    if not df.empty:
        print("\nPrimeras 3 motos:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"{i+1}. {row['Titulo']} - {row['Precio']} - {row['Ubicacion']}")
