"""
================================================================================
                    SCRAPER CB125R SIMPLIFICADO Y FUNCIONAL                     
================================================================================

Version: SIMPLIFICADA EXTREMA
Objetivo: CONSEGUIR DATOS REALES, no optimizaciones

ESTRATEGIA:
1. Extractores ultra simples que siempre devuelvan algo
2. Validación mínima que acepte casi todo
3. Debug extenso para ver qué pasa
4. Solo las URLs más básicas

================================================================================
"""

import re
import time
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import Dict, List, Optional
from tqdm import tqdm

# Imports del sistema
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_modelo_config, is_github_actions

class ScraperCB125RSimplificado:
    """Scraper CB125R ULTRA SIMPLIFICADO para conseguir datos reales"""
    
    def __init__(self):
        self.modelo_key = 'cb125r'
        self.modelo_config = get_modelo_config('cb125r')
        self.logger = logging.getLogger(__name__)
        
        self.driver = None
        self.results = []
        
        # CONTADORES PARA DEBUG DETALLADO
        self.debug_stats = {
            'urls_procesadas': 0,
            'anuncios_encontrados': 0,
            'anuncios_visitados': 0,
            'datos_extraidos_ok': 0,
            'datos_extraidos_vacios': 0,
            'validaciones_exitosas': 0,
            'validaciones_fallidas': 0,
            'errores_navegacion': 0
        }
        
        print(" SCRAPER CB125R ULTRA SIMPLIFICADO INICIADO")
        print(" Objetivo: Conseguir CUALQUIER dato real de CB125R")
        print(" Debug: Logging extenso activado")
    
    def setup_driver(self):
        """Setup básico del driver"""
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        
        if is_github_actions():
            options.add_argument("--headless")
        
        # Suprimir logs
        options.add_argument("--log-level=3")
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        
        try:
            driver = webdriver.Chrome(options=options)
            driver.implicitly_wait(2)
            driver.set_page_load_timeout(30)
            
            print(" Driver configurado correctamente")
            return driver
        except Exception as e:
            print(f" Error configurando driver: {e}")
            raise
    
    def get_basic_urls(self) -> List[str]:
        """URLs ultra básicas - solo las más simples"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        # SOLO LAS URLs MÁS BÁSICAS
        urls = [
            f"https://es.wallapop.com/app/search?keywords=honda%20cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            f"https://es.wallapop.com/app/search?keywords=cb125r&min_sale_price={min_price}&max_sale_price={max_price}",
            "https://es.wallapop.com/app/search?keywords=honda%20cb125r",
            "https://es.wallapop.com/app/search?keywords=cb125r",
            f"https://es.wallapop.com/app/search?keywords=honda%20cb%20125%20r&min_sale_price={min_price}&max_sale_price={max_price}"
        ]
        
        print(f" {len(urls)} URLs básicas generadas")
        return urls
    
    def scrape_basic(self) -> pd.DataFrame:
        """Scraping básico sin optimizaciones"""
        try:
            print("\n INICIANDO SCRAPING BÁSICO")
            
            # Setup driver
            self.driver = self.setup_driver()
            
            # URLs básicas
            urls = self.get_basic_urls()
            
            for i, url in enumerate(urls, 1):
                print(f"\n [{i}/{len(urls)}] Procesando: {url[:60]}...")
                
                try:
                    self.process_url_basic(url, i)
                    self.debug_stats['urls_procesadas'] += 1
                    time.sleep(2)  # Pausa entre URLs
                except Exception as e:
                    print(f"  Error en URL {i}: {e}")
                    continue
            
            # Resultado final
            if self.results:
                df = pd.DataFrame(self.results)
                print(f"\n SCRAPING COMPLETADO: {len(df)} motos encontradas")
                self.show_debug_stats()
                self.show_sample_results(df)
                return df
            else:
                print(f"\n NO SE ENCONTRARON RESULTADOS")
                self.show_debug_stats()
                return pd.DataFrame()
                
        except Exception as e:
            print(f" Error crítico: {e}")
            return pd.DataFrame()
        finally:
            if self.driver:
                self.driver.quit()
                print(" Driver cerrado")
    
    def process_url_basic(self, url: str, url_num: int):
        """Procesar una URL de forma básica"""
        try:
            # Navegar
            self.driver.get(url)
            time.sleep(3)
            
            # Aceptar cookies si aparece
            try:
                cookie_btn = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                cookie_btn.click()
                time.sleep(1)
            except:
                pass
            
            # Scroll básico
            for i in range(3):
                self.driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(0.5)
            
            # Obtener enlaces
            enlaces = self.get_enlaces_basic()
            self.debug_stats['anuncios_encontrados'] += len(enlaces)
            
            print(f" {len(enlaces)} enlaces encontrados en URL {url_num}")
            
            if not enlaces:
                print(f"  No se encontraron enlaces en URL {url_num}")
                return
            
            # Procesar anuncios (solo los primeros 10 para testing)
            max_anuncios = 10
            enlaces_procesar = enlaces[:max_anuncios]
            
            print(f" Procesando {len(enlaces_procesar)} anuncios...")
            
            for j, enlace in enumerate(enlaces_procesar, 1):
                try:
                    print(f"    [{j}/{len(enlaces_procesar)}] Analizando anuncio...")
                    
                    moto_data = self.extract_anuncio_basic(enlace)
                    self.debug_stats['anuncios_visitados'] += 1
                    
                    if moto_data:
                        self.debug_stats['datos_extraidos_ok'] += 1
                        
                        # MOSTRAR DATOS EXTRAÍDOS (debug)
                        print(f"       Datos extraídos:")
                        print(f"         Titulo: '{moto_data.get('Titulo', 'N/A')}'")
                        print(f"         Precio: '{moto_data.get('Precio', 'N/A')}'")
                        print(f"         Año: '{moto_data.get('Año', 'N/A')}'")
                        
                        # Validación ultra simple
                        if self.validate_basic(moto_data):
                            self.results.append(moto_data)
                            self.debug_stats['validaciones_exitosas'] += 1
                            print(f"       MOTO VÁLIDA AGREGADA!")
                        else:
                            self.debug_stats['validaciones_fallidas'] += 1
                            print(f"       Rechazada en validación")
                    else:
                        self.debug_stats['datos_extraidos_vacios'] += 1
                        print(f"        No se pudieron extraer datos")
                    
                    time.sleep(1)  # Pausa entre anuncios
                    
                except Exception as e:
                    self.debug_stats['errores_navegacion'] += 1
                    print(f"       Error: {e}")
                    continue
                    
        except Exception as e:
            print(f" Error procesando URL: {e}")
    
    def get_enlaces_basic(self) -> List[str]:
        """Obtener enlaces de forma básica"""
        enlaces = []
        
        try:
            # Selector ultra simple
            elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/item/"]')
            
            for element in elements:
                href = element.get_attribute('href')
                if href and '/item/' in href:
                    enlaces.append(href)
            
            return list(set(enlaces))  # Eliminar duplicados
            
        except Exception as e:
            print(f"Error obteniendo enlaces: {e}")
            return []
    
    def extract_anuncio_basic(self, url: str) -> Optional[Dict]:
        """Extracción ULTRA BÁSICA de datos"""
        try:
            # Navegar al anuncio
            self.driver.get(url)
            time.sleep(2)
            
            # Extraer datos básicos
            data = {
                'URL': url,
                'Titulo': self.extract_titulo_ultra_simple(),
                'Precio': self.extract_precio_ultra_simple(),
                'Año': self.extract_año_ultra_simple(),
                'Kilometraje': self.extract_km_ultra_simple(),
                'Vendedor': "Particular",  # Por defecto
                'Ubicacion': "No especificado",  # Simplificado
                'Fecha_Publicacion': "No especificado",  # Simplificado
                'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return data
            
        except Exception as e:
            print(f"Error extrayendo datos: {e}")
            return None
    
    def extract_titulo_ultra_simple(self) -> str:
        """Extractor de título ULTRA SIMPLE"""
        try:
            # Buscar cualquier H1
            h1_elements = self.driver.find_elements(By.TAG_NAME, "h1")
            for h1 in h1_elements:
                text = h1.text.strip()
                if text and len(text) > 5:
                    return text
            
            return "Sin titulo"
        except:
            return "Sin titulo"
    
    def extract_precio_ultra_simple(self) -> str:
        """Extractor de precio ULTRA SIMPLE"""
        try:
            # Buscar cualquier elemento con €
            elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
            
            for element in elements[:5]:  # Solo los primeros 5
                text = element.text.strip()
                if text and '€' in text and len(text) < 20:
                    # Extraer números
                    numbers = re.findall(r'\d+', text.replace('.', '').replace(',', ''))
                    if numbers:
                        try:
                            price = int(numbers[0])
                            if 100 <= price <= 50000:
                                return f"{price} €"
                        except:
                            continue
            
            return "No especificado"
        except:
            return "No especificado"
    
    def extract_año_ultra_simple(self) -> str:
        """Extractor de año ULTRA SIMPLE"""
        try:
            # Buscar año en el título
            titulo = self.extract_titulo_ultra_simple()
            years = re.findall(r'\b(20[1-2][0-9])\b', titulo)
            
            for year in years:
                year_int = int(year)
                if 2015 <= year_int <= 2025:
                    return year
            
            return "No especificado"
        except:
            return "No especificado"
    
    def extract_km_ultra_simple(self) -> str:
        """Extractor de km ULTRA SIMPLE"""
        try:
            # Buscar en el source de la página
            page_source = self.driver.page_source.lower()
            
            # Patrones simples
            km_matches = re.findall(r'(\d{1,6})\s*km', page_source)
            
            for km_str in km_matches:
                try:
                    km = int(km_str)
                    if 0 <= km <= 200000:
                        return f"{km} km"
                except:
                    continue
            
            return "No especificado"
        except:
            return "No especificado"
    
    def validate_basic(self, moto_data: Dict) -> bool:
        """Validación ULTRA BÁSICA - acepta casi todo"""
        try:
            titulo = moto_data.get('Titulo', '').lower()
            precio = moto_data.get('Precio', '')
            
            # CRITERIO MÍNIMO: título no vacío
            if not titulo or titulo == "sin titulo":
                print(f"          Título vacío")
                return False
            
            # CRITERIO MÍNIMO: debe tener alguna palabra relacionada con motos o Honda
            keywords_motos = ['honda', 'moto', 'cb', '125', 'r']
            tiene_keyword = any(keyword in titulo for keyword in keywords_motos)
            
            if not tiene_keyword:
                print(f"          No parece ser una moto Honda")
                return False
            
            # CRITERIO OPCIONAL: precio válido (no obligatorio)
            if precio != "No especificado":
                try:
                    price_numbers = re.findall(r'\d+', precio)
                    if price_numbers:
                        price = int(price_numbers[0])
                        if price < 500 or price > 15000:
                            print(f"         ⚠  Precio fuera de rango pero acepto: {precio}")
                except:
                    pass
            
            print(f"          Validación básica OK")
            return True
            
        except Exception as e:
            print(f"         Error en validación: {e}")
            return False
    
    def show_debug_stats(self):
        """Mostrar estadísticas de debug"""
        stats = self.debug_stats
        
        print("\n" + "="*60)
        print(" ESTADÍSTICAS DE DEBUG")
        print("="*60)
        print(f"URLs procesadas: {stats['urls_procesadas']}")
        print(f"Anuncios encontrados: {stats['anuncios_encontrados']}")
        print(f"Anuncios visitados: {stats['anuncios_visitados']}")
        print(f"Datos extraídos OK: {stats['datos_extraidos_ok']}")
        print(f"Datos extraídos vacíos: {stats['datos_extraidos_vacios']}")
        print(f"Validaciones exitosas: {stats['validaciones_exitosas']}")
        print(f"Validaciones fallidas: {stats['validaciones_fallidas']}")
        print(f"Errores de navegación: {stats['errores_navegacion']}")
        
        # Análisis
        if stats['anuncios_encontrados'] > 0:
            tasa_visita = (stats['anuncios_visitados'] / stats['anuncios_encontrados']) * 100
            print(f"\nTasa de visita: {tasa_visita:.1f}%")
        
        if stats['anuncios_visitados'] > 0:
            tasa_extraccion = (stats['datos_extraidos_ok'] / stats['anuncios_visitados']) * 100
            print(f"Tasa de extracción: {tasa_extraccion:.1f}%")
        
        if stats['datos_extraidos_ok'] > 0:
            tasa_validacion = (stats['validaciones_exitosas'] / stats['datos_extraidos_ok']) * 100
            print(f"Tasa de validación: {tasa_validacion:.1f}%")
        
        print("="*60)
    
    def show_sample_results(self, df: pd.DataFrame):
        """Mostrar muestra de resultados"""
        if not df.empty:
            print("\n MUESTRA DE RESULTADOS:")
            for i, (_, row) in enumerate(df.head(5).iterrows(), 1):
                print(f"{i}. {row['Titulo'][:50]}...")
                print(f"   Precio: {row['Precio']} | Año: {row['Año']} | KM: {row['Kilometraje']}")
                print(f"   URL: {row['URL'][:60]}...")
                print()


def run_cb125r_simplificado():
    """Función principal simplificada"""
    print("="*80)
    print("    SCRAPER CB125R ULTRA SIMPLIFICADO")
    print("="*80)
    print(" OBJETIVO: Conseguir CUALQUIER dato real de CB125R")
    print(" ESTRATEGIA: Máxima simplicidad, mínima optimización")
    print(" DEBUG: Logging detallado de cada paso")
    print("="*80)
    
    try:
        scraper = ScraperCB125RSimplificado()
        df_results = scraper.scrape_basic()
        
        if not df_results.empty:
            print(f"\n ÉXITO: {len(df_results)} motos CB125R encontradas")
            return df_results
        else:
            print(f"\n ANÁLISIS NECESARIO: Ver estadísticas arriba")
            return df_results
            
    except Exception as e:
        print(f"\n ERROR CRÍTICO: {e}")
        return None

if __name__ == "__main__":
    results = run_cb125r_simplificado()
    
    if results is not None and not results.empty:
        print(f"\n RESULTADO FINAL: {len(results)} motos extraídas exitosamente")
        
        # Guardar resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cb125r_simplificado_{timestamp}.xlsx"
        results.to_excel(filename, index=False)
        print(f" Resultados guardados en: {filename}")
    else:
        print(f"\n ANÁLISIS: Revisar estadísticas de debug arriba")
        print(" SIGUIENTE PASO: Identificar dónde está fallando el proceso")
