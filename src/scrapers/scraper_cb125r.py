"""
================================================================================
                       SCRAPER CB125R INTEGRADO CON ARQUITECTURA                
================================================================================

Scraper específico para Honda CB125R que sigue la arquitectura del sistema
- Hereda de BaseScraper
- Incluye selectores CSS actualizados para Wallapop 2025
- Botón "Cargar más" funcional
- Scroll inteligente integrado
- Validación de snippets optimizada

Autor: Sistema Integrado
Versión: 3.0
Fecha: Septiembre 2025

================================================================================
"""

import re
import time
import logging
from typing import Dict, List, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapers.base_scraper import BaseScraper

class ScraperCB125R(BaseScraper):
    """Scraper específico para Honda CB125R - INTEGRADO CON ARQUITECTURA"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Stats de validación para debug
        self.validation_stats = {
            'total_processed': 0,
            'empty_title': 0,
            'no_honda': 0, 
            'no_cb125r': 0,
            'excluded_model': 0,
            'invalid_price': 0,
            'successful': 0
        }
        
        self.logger.info(f"Scraper CB125R inicializado - Debug activado")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs optimizadas para Honda CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        self.logger.info(f"Generadas 12 URLs para CB125R")
        
        base = "https://es.wallapop.com/app/search?"
        price_filter = f"min_sale_price={min_price}&max_sale_price={max_price}"
        
        # URLs optimizadas para CB125R
        urls = [
            # Básicas principales
            f"{base}keywords=honda%20cb125r&{price_filter}",
            f"{base}keywords=cb125r&{price_filter}",
            f"{base}keywords=honda%20cb%20125%20r&{price_filter}",
            
            # Con ordenación
            f"{base}keywords=honda%20cb125r&{price_filter}&order_by=newest",
            f"{base}keywords=honda%20cb125r&{price_filter}&order_by=price_low_to_high",
            
            # Años recientes más comunes
            f"{base}keywords=honda%20cb125r%202022&{price_filter}",
            f"{base}keywords=honda%20cb125r%202021&{price_filter}",
            f"{base}keywords=honda%20cb125r%202020&{price_filter}",
            
            # Términos alternativos
            f"{base}keywords=moto%20honda%20cb125r&{price_filter}",
            f"{base}keywords=honda%20deportiva%20125&{price_filter}",
            
            # Errores comunes
            f"{base}keywords=handa%20cb%20125%20r&{price_filter}",
            f"{base}keywords=cb%20125%20r%20honda&{price_filter}"
        ]
        
        return urls
    
    def _click_load_more_corrected(self) -> bool:
        """CORREGIDO: Buscar y hacer clic en botón 'Cargar más' - SELECTORES 2025"""
        try:
            # Scroll al final para que aparezca el botón
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # SELECTORES ACTUALIZADOS PARA WALLAPOP 2025
            selectors = [
                # Selector específico del botón walla-button
                ('css', 'walla-button[text="Cargar más"]'),
                ('css', 'walla-button[button-type="primary"]'),
                ('css', '.search-page-results_SearchPageResults__loadMore__A_eRR walla-button'),
                
                # Selectores del botón interno
                ('css', 'button.walla-button__button--primary'),
                ('css', 'button.walla-button__button'),
                
                # XPath para texto específico
                ('xpath', '//span[text()="Cargar más"]/ancestor::button'),
                ('xpath', '//walla-button[@text="Cargar más"]'),
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
                                # Scroll al elemento y hacer clic
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
            
        except Exception as e:
            self.logger.debug(f"Error haciendo clic en 'Cargar más': {e}")
            return False
    
    def _smart_scroll_and_load(self, target_count: int = 60) -> int:
        """CORREGIDO: Scroll inteligente con botón 'Cargar más' funcional"""
        
        # Contar enlaces iniciales
        initial_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        self.logger.info(f"[SMART] Enlaces iniciales: {initial_count}")
        
        # FASE 1: Scroll inicial suave
        for i in range(10):
            self.driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.2)
        
        current_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        self.logger.info(f"[SMART] Tras scroll inicial: {current_count}")
        
        # FASE 2: Intentar clic en "Cargar más"
        clicks_realizados = 0
        max_clicks = 3
        
        while clicks_realizados < max_clicks and current_count < target_count:
            if self._click_load_more_corrected():
                clicks_realizados += 1
                time.sleep(3)  # Esperar a que cargue
                
                new_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
                
                if new_count > current_count:
                    gained = new_count - current_count
                    self.logger.info(f"[SMART] Clic {clicks_realizados}: {current_count} -> {new_count} (+{gained})")
                    current_count = new_count
                    
                    # Scroll después del clic para revelar nuevos elementos
                    for scroll in range(8):
                        self.driver.execute_script("window.scrollBy(0, 1000);")
                        time.sleep(0.3)
                    
                    if new_count >= target_count:
                        self.logger.info(f"[SMART] Objetivo alcanzado: {new_count} anuncios")
                        break
                else:
                    self.logger.info(f"[SMART] Sin nuevos anuncios, fin del contenido")
                    break
            else:
                self.logger.info(f"[SMART] Botón no encontrado, fin del contenido")
                break
        
        # FASE 3: Scroll final intensivo (como solicitas)
        self.logger.info(f"[SMART] Iniciando scroll intensivo...")
        for scroll_num in range(50):  # Scroll intensivo
            self.driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.1)
            
            if scroll_num % 10 == 0:
                scroll_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
                self.logger.info(f"[SMART] Scroll {scroll_num}: {scroll_count} anuncios")
        
        final_count = len(self.driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        self.logger.info(f"[SMART] Total final: {final_count} anuncios ({clicks_realizados} clics)")
        
        return final_count
    
    def _extract_title_from_snippet_FIXED(self, element):
        """CORREGIDO: Extraer título usando selectores actualizados 2025"""
        try:
            # SELECTORES ACTUALIZADOS PARA WALLAPOP 2025
            title_selectors = [
                "h3.item-card_ItemCard__title__5TocV",  # SELECTOR REAL 2025
                "h3[class*='ItemCard__title']",         # Patrón flexible
                "h3[class*='title']",                    # Genérico
                "h3",                                    # Fallback
                "*[class*='title'] h3",
                "*[aria-label*='title']"
            ]
            
            for selector in title_selectors:
                try:
                    title_elem = element.find_element(By.CSS_SELECTOR, selector)
                    title = title_elem.text.strip()
                    if title and len(title) > 3:
                        return title.lower()
                except:
                    continue
            
            # XPath como respaldo
            xpath_selectors = [
                ".//h3[contains(@class, 'title')]",
                ".//h3",
                ".//*[contains(text(), 'Honda') or contains(text(), 'CB125')]"
            ]
            
            for xpath in xpath_selectors:
                try:
                    title_elem = element.find_element(By.XPATH, xpath)
                    title = title_elem.text.strip()
                    if title and len(title) > 3:
                        return title.lower()
                except:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def _extract_price_from_snippet_FIXED(self, element):
        """CORREGIDO: Extraer precio usando selectores actualizados 2025"""
        try:
            # SELECTORES ACTUALIZADOS PARA PRECIO 2025
            price_selectors = [
                "strong.item-card_ItemCard__price__pVpdc",  # SELECTOR REAL 2025
                "strong[class*='ItemCard__price']",         # Patrón flexible
                "strong[class*='price']",                    # Genérico
                "*[aria-label*='Item price']",               # Por aria-label
                "*[class*='price']",                         # Muy genérico
                "strong"                                     # Fallback
            ]
            
            for selector in price_selectors:
                try:
                    price_elem = element.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_elem.text.strip()
                    if '€' in price_text:
                        return price_text
                except:
                    continue
            
            # XPath como respaldo
            xpath_selectors = [
                ".//*[contains(text(), '€')]",
                ".//*[@aria-label='Item price']"
            ]
            
            for xpath in xpath_selectors:
                try:
                    price_elem = element.find_element(By.XPATH, xpath)
                    price_text = price_elem.text.strip()
                    if '€' in price_text:
                        return price_text
                except:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def _validate_snippet_cb125r(self, element):
        """Validar snippet específicamente para Honda CB125R"""
        try:
            # Extraer título con selectores corregidos
            title = self._extract_title_from_snippet_FIXED(element)
            if not title or len(title) < 3:
                self.validation_stats['empty_title'] += 1
                return False
            
            # Verificar que menciona Honda o CB125R
            honda_keywords = ['honda', 'handa']  # Incluir error común
            cb125r_keywords = ['cb125r', 'cb 125 r', 'cb 125r', 'cb-125-r']
            
            has_honda = any(keyword in title for keyword in honda_keywords)
            has_cb125r = any(keyword in title for keyword in cb125r_keywords)
            
            if not (has_honda or has_cb125r):
                self.validation_stats['no_honda'] += 1
                return False
            
            # Verificar que es específicamente CB125R
            if not has_cb125r:
                self.validation_stats['no_cb125r'] += 1
                return False
            
            # Verificar precio en rango
            price_text = self._extract_price_from_snippet_FIXED(element)
            if price_text:
                price_numbers = re.findall(r'\d+', price_text.replace('.', '').replace(',', ''))
                if price_numbers:
                    price_value = int(''.join(price_numbers))
                    min_price = self.modelo_config['precio_min']
                    max_price = self.modelo_config['precio_max']
                    
                    if not (min_price <= price_value <= max_price):
                        self.validation_stats['invalid_price'] += 1
                        return False
            
            self.validation_stats['successful'] += 1
            return True
            
        except Exception as e:
            return False
    
    def validate_anuncio(self, titulo: str, descripcion: str) -> bool:
        """Validar anuncio completo Honda CB125R"""
        try:
            self.validation_stats['total_processed'] += 1
            
            # Verificar título
            if not titulo or len(titulo.strip()) < 5:
                self.validation_stats['empty_title'] += 1
                self.logger.info(f"   Rechazado: título vacío")
                return False
            
            texto_completo = (titulo + " " + descripcion).lower()
            
            # Verificar que es Honda
            if not any(marca in texto_completo for marca in ['honda', 'handa']):
                self.validation_stats['no_honda'] += 1
                self.logger.info(f"   Rechazado: sin Honda")
                return False
            
            # Verificar que es CB125R específicamente
            cb125r_patterns = [
                r'\bcb[\s\-]*125[\s\-]*r\b',
                r'\bcb125r\b',
                r'\bcb\s*125\s*r\b'
            ]
            
            if not any(re.search(pattern, texto_completo) for pattern in cb125r_patterns):
                self.validation_stats['no_cb125r'] += 1
                self.logger.info(f"   Rechazado: sin CB125R")
                return False
            
            # Excluir otros modelos Honda
            excluded_patterns = [
                r'\bcb[\s\-]*50\b', r'\bcb[\s\-]*100\b', r'\bcb[\s\-]*250\b', 
                r'\bcb[\s\-]*300\b', r'\bcb[\s\-]*400\b', r'\bcb[\s\-]*500\b',
                r'\bcb[\s\-]*600\b', r'\bcb[\s\-]*650\b', r'\bcb[\s\-]*900\b',
                r'\bcbr\b', r'\bhornet\b', r'\bvaradero\b'
            ]
            
            if any(re.search(pattern, texto_completo) for pattern in excluded_patterns):
                self.validation_stats['excluded_model'] += 1
                self.logger.info(f"   Rechazado: modelo excluido")
                return False
            
            self.validation_stats['successful'] += 1
            self.logger.info(f"   Aceptado como Honda CB125R válida")
            return True
            
        except Exception as e:
            self.logger.debug(f"Error en validación: {e}")
            return False
    
    def _extract_titulo_corrected(self) -> str:
        """CORREGIDO: Extraer título con selectores actualizados"""
        try:
            # Esperar a que cargue
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            
            # SELECTORES ACTUALIZADOS PARA PÁGINA DE DETALLE 2025
            selectors = [
                'h1[class*="ItemDetailTwoColumns__title"]',  # Selector específico
                'h1[class*="title"]',
                'h1[data-testid="item-title"]',
                'h1',
                '*[class*="title"] h1'
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
        """CORREGIDO: Extraer precio con selectores actualizados"""
        try:
            # SELECTORES ACTUALIZADOS PARA PRECIO EN PÁGINA DE DETALLE
            selectors = [
                'span[class*="ItemDetailPrice--standard"]',
                '*[class*="price"]',
                '*[aria-label*="Item Price"]',
                'span.price',
                'strong[class*="price"]'
            ]
            
            for selector in selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.text.strip():
                        price_text = element.text.strip()
                        if '€' in price_text:
                            return price_text
                except:
                    continue
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _extract_kilometraje_corrected(self) -> str:
        """Extraer kilometraje mejorado"""
        try:
            # Buscar en toda la página
            page_text = self.driver.page_source.lower()
            
            # Patrones para kilometraje
            km_patterns = [
                r'(\d{1,3}(?:\.\d{3})*|\d+)\s*(?:km|kms|kilometros|kilómetros)',
                r'km[:\s]*(\d{1,6})',
                r'(\d+)\s*mil\s*(?:km|kms|kilometros)?'
            ]
            
            for pattern in km_patterns:
                matches = re.finditer(pattern, page_text)
                for match in matches:
                    km_text = match.group(1) if match.groups() else match.group(0)
                    if km_text and km_text.isdigit():
                        km_value = int(km_text.replace('.', ''))
                        if 1 <= km_value <= 100000:  # Rango razonable
                            return f"{km_value} km"
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _extract_año_corrected(self) -> str:
        """Extraer año mejorado"""
        try:
            page_text = self.driver.page_source.lower()
            
            # Patrones para año
            year_patterns = [
                r'año[:\s]*(20\d{2})',
                r'modelo[:\s]*(20\d{2})',
                r'(20\d{2})\s*honda',
                r'honda[^.]*?(20\d{2})',
                r'\b(20[0-2][0-9])\b'  # 2000-2029
            ]
            
            for pattern in year_patterns:
                matches = re.finditer(pattern, page_text)
                for match in matches:
                    year = match.group(1)
                    if year and 2010 <= int(year) <= 2025:  # Rango CB125R
                        return year
            
            return "No especificado"
            
        except Exception as e:
            return "No especificado"
    
    def _show_validation_summary(self):
        """Mostrar resumen de validación CB125R"""
        stats = self.validation_stats
        total = stats['total_processed']
        
        self.logger.info("="*60)
        self.logger.info("RESUMEN DE VALIDACIÓN CB125R")
        self.logger.info("="*60)
        self.logger.info(f"Total procesados: {total}")
        self.logger.info(f"Exitosos: {stats['successful']} ({stats['successful']/total*100:.1f}%)")
        self.logger.info(f"RECHAZOS:")
        self.logger.info(f"  Título vacío: {stats['empty_title']} ({stats['empty_title']/total*100:.1f}%)")
        self.logger.info(f"  Sin Honda: {stats['no_honda']} ({stats['no_honda']/total*100:.1f}%)")
        self.logger.info(f"  Sin CB125R: {stats['no_cb125r']} ({stats['no_cb125r']/total*100:.1f}%)")
        self.logger.info(f"  Modelo excluido: {stats['excluded_model']} ({stats['excluded_model']/total*100:.1f}%)")
        self.logger.info("="*60)
        
        if stats['successful'] == 0:
            self.logger.warning("⚠️ NINGUNA VALIDACIÓN EXITOSA")
            self.logger.warning("   - PROBLEMA: Extractor de títulos fallando")

# FUNCIÓN REQUERIDA POR LA ARQUITECTURA
def run_cb125r_scraper() -> pd.DataFrame:
    """
    Función de ejecución directa requerida por la arquitectura del sistema
    
    Returns:
        DataFrame con resultados del scraping CB125R
    """
    import pandas as pd
    
    try:
        # Crear instancia del scraper
        scraper = ScraperCB125R()
        
        # Configurar driver
        scraper.setup_driver()
        
        # Ejecutar scraping
        scraper.logger.info("Iniciando scraping CORREGIDO de Honda CB125R")
        results = scraper.run_scraping()
        
        # Convertir a DataFrame
        if results:
            df = pd.DataFrame(results)
            scraper.logger.info(f"Scraping completado: {len(df)} Honda CB125R encontradas")
            return df
        else:
            scraper.logger.warning("No se encontraron resultados")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error en run_cb125r_scraper: {e}")
        return pd.DataFrame()
    finally:
        if 'scraper' in locals() and scraper.driver:
            scraper.driver.quit()

if __name__ == "__main__":
    # Test directo
    print("Testing Honda CB125R Scraper...")
    df = run_cb125r_scraper()
    print(f"Resultados obtenidos: {len(df)}")
