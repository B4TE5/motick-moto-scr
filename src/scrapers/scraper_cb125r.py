"""
HONDA CB125R SCRAPER CORREGIDO - VERSIÓN 2.5 ULTRA OPTIMIZADA
=============================================================

CORRECCIONES IMPLEMENTADAS:
 Selectores CSS actualizados para Wallapop 2025
 Extractor de títulos CORREGIDO
 Botón "Cargar más" FUNCIONAL
 90+ URLs optimizadas restauradas
 Scroll inteligente implementado
 Filtrado en snippet para eficiencia

Autor: Sistema Corregido
Fecha: Septiembre 2025
"""

import time
import re
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from colorama import Fore, Back, Style, init
from tqdm import tqdm
import unicodedata

# Inicializar colorama
init(autoreset=True)

def setup_browser():
    """Configurar navegador Chrome OPTIMIZADO"""
    options = Options()
    
    # CONFIGURACIÓN ULTRA OPTIMIZADA
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-images")  # CRÍTICO: No cargar imágenes
    options.add_argument("--disable-javascript")  # OPCIONAL: Comentar si causa problemas
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
    
    # Suprimir logs
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    browser = webdriver.Chrome(options=options)
    browser.maximize_window()
    browser.implicitly_wait(2)
    return browser

def get_search_urls_optimized(modelo: str, min_price: int, max_price: int) -> list[str]:
    """RESTAURAR 90+ URLs OPTIMIZADAS PARA CB125R"""
    urls = []
    
    print(f"[DEBUG] Generando 90 URLs OPTIMIZADAS para {modelo} (precio {min_price}€-{max_price}€)...")
    
    base = "https://es.wallapop.com/app/search?"
    price_filter = f"min_sale_price={min_price}&max_sale_price={max_price}"
    
    # 1. BÚSQUEDAS PRINCIPALES (10 URLs)
    core_searches = [
        "honda%20cb%20125%20r",
        "cb%20125%20r", 
        "cb125r",
        "honda%20cb125r",
        "honda%20125%20r",
        "cb%20125r",
        "honda%20cb%20125r",
        "cb-125-r",
        "CB%20125%20R",
        "honda%20CB125R"
    ]
    
    for search in core_searches:
        urls.append(f"{base}keywords={search}&{price_filter}")
    
    # 2. AÑOS MÁS IMPORTANTES (36 URLs)
    recent_years = [2020, 2021, 2022, 2023, 2024]
    main_terms = ["honda%20cb%20125%20r", "cb125r", "cb%20125%20r"]
    
    for year in recent_years:
        for term in main_terms:
            urls.append(f"{base}keywords={term}%20{year}&{price_filter}")
    
    mid_years = [2016, 2017, 2018, 2019]
    mid_terms = ["honda%20cb%20125%20r", "cb125r"]
    
    for year in mid_years:
        for term in mid_terms:
            urls.append(f"{base}keywords={term}%20{year}&{price_filter}")
    
    old_years = [2013, 2014, 2015]
    for year in old_years:
        urls.append(f"{base}keywords=honda%20cb%20125%20r%20{year}&{price_filter}")
    
    # 3. CIUDADES TOP (20 URLs)
    cities_top = [
        (40.4168, -3.7038, "Madrid"),
        (41.3851, 2.1734, "Barcelona"),  
        (39.4699, -0.3763, "Valencia"),
        (37.3886, -5.9823, "Sevilla"),
        (43.2627, -2.9253, "Bilbao"),
        (36.7213, -4.4214, "Málaga"),
        (41.6563, -0.8773, "Zaragoza"),
        (37.1735, -3.5986, "Granada"),
        (43.3614, -5.8593, "Oviedo"),
        (38.3452, -0.4810, "Alicante")
    ]
    
    city_terms = ["honda%20cb%20125%20r", "cb125r"]
    
    for lat, lng, city in cities_top:
        for term in city_terms:
            urls.append(f"{base}keywords={term}&latitude={lat}&longitude={lng}&{price_filter}")
    
    # 4. BÚSQUEDAS ESPECIALES (15 URLs)
    special_searches = [
        "honda%20cb%20125%20r%20pocos%20km",
        "cb125r%20impecable", 
        "honda%20cb125r%20abs",
        "cb%20125%20r%20deportiva",
        "honda%20cb%20125%20r%20oportunidad",
        "cb125r%20urge",
        "honda%20cb%20125%20r%20chollo",
        "cb%20125%20r%20ganga",
        "honda%20cb125r%20perfecta",
        "cb%20125%20r%20cuidada",
        "honda%20cb%20125%20r%20revisada",
        "cb125r%20original",
        "honda%20cb%20125%20r%20única",
        "cb125r%20estado%20perfecto",
        "honda%20cb125r%20pocos%20kilómetros"
    ]
    
    for search in special_searches:
        urls.append(f"{base}keywords={search}&{price_filter}")
    
    # 5. FILTROS Y RANGOS (6 URLs)
    filtered_searches = [
        f"{base}keywords=honda%20cb%20125%20r&max_kilometers=15000&{price_filter}",
        f"{base}keywords=cb125r&min_kilometers=0&max_kilometers=20000&{price_filter}",
        f"{base}keywords=honda%20cb%20125%20r&min_sale_price=1500&max_sale_price=2800",
        f"{base}keywords=cb125r&min_sale_price=3200&max_sale_price=5000",
        f"{base}keywords=honda%20cb%20125%20r&order_by=price_low_to_high",
        f"{base}keywords=cb125r&order_by=newest&{price_filter}"
    ]
    
    urls.extend(filtered_searches)
    
    # 6. ERRORES COMUNES (3 URLs)
    typo_searches = [
        "handa%20cb%20125%20r",
        "honda%20cv%20125%20r",
        "cb%20125%20r%20honda"
    ]
    
    for search in typo_searches:
        urls.append(f"{base}keywords={search}&{price_filter}")
    
    # Eliminar duplicados
    unique_urls = sorted(set(urls))
    final_urls = unique_urls[:90]  # Exactamente 90
    
    print(f"URLs OPTIMIZADAS generadas: {len(final_urls)}")
    return final_urls

def extract_title_from_snippet_FIXED(element):
    """CORREGIDO: Extraer título del snippet usando selectores ACTUALIZADOS"""
    try:
        # SELECTORES ACTUALIZADOS PARA WALLAPOP 2025
        title_selectors = [
            "h3.item-card_ItemCard__title__5TocV",  # SELECTOR REAL de 2025
            "h3[class*='ItemCard__title']",         # Patrón flexible
            "h3[class*='title']",                    # Genérico
            "h3",                                    # Fallback
            "*[class*='title'] h3",
            "*[class*='Title'] h3"
        ]
        
        for selector in title_selectors:
            try:
                title_elem = element.find_element(By.CSS_SELECTOR, selector)
                title = title_elem.text.strip()
                if title and len(title) > 3:
                    print(f" TÍTULO EXTRAÍDO: {title}")
                    return title
            except Exception as e:
                print(f" Selector {selector} falló: {e}")
                continue
        
        # Si no encuentra con CSS, probar con XPath
        xpath_selectors = [
            ".//h3[contains(@class, 'title')]",
            ".//h3",
            ".//*[contains(text(), 'CB125R') or contains(text(), 'Honda')]"
        ]
        
        for xpath in xpath_selectors:
            try:
                title_elem = element.find_element(By.XPATH, xpath)
                title = title_elem.text.strip()
                if title and len(title) > 3:
                    print(f" TÍTULO XPATH: {title}")
                    return title
            except:
                continue
        
        print(" NO SE PUDO EXTRAER TÍTULO")
        return None
        
    except Exception as e:
        print(f" ERROR EXTRAYENDO TÍTULO: {e}")
        return None

def extract_price_from_snippet_FIXED(element):
    """CORREGIDO: Extraer precio del snippet"""
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
                    print(f" PRECIO EXTRAÍDO: {price_text}")
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
                    print(f" PRECIO XPATH: {price_text}")
                    return price_text
            except:
                continue
        
        print("❌ NO SE PUDO EXTRAER PRECIO")
        return None
        
    except Exception as e:
        print(f"❌ ERROR EXTRAYENDO PRECIO: {e}")
        return None

def click_load_more_button_FIXED(driver):
    """CORREGIDO: Hacer clic en botón 'Cargar más' con selectores ACTUALIZADOS"""
    try:
        print(" Buscando botón 'Cargar más'...")
        
        # Scroll al final primero
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # SELECTORES ACTUALIZADOS PARA WALLAPOP 2025
        button_selectors = [
            # Selector específico para walla-button 2025
            'walla-button[text="Cargar más"]',
            'walla-button[button-type="primary"]',
            
            # Dentro del contenedor específico
            '.search-page-results_SearchPageResults__loadMore__A_eRR walla-button',
            
            # Por clase del botón interno
            '.walla-button__button--primary',
            'button.walla-button__button',
            
            # Genéricos
            'walla-button',
            'button[class*="walla-button"]'
        ]
        
        # XPath selectors como respaldo
        xpath_selectors = [
            '//walla-button[@text="Cargar más"]',
            '//walla-button[contains(@text, "Cargar")]',
            '//button[contains(@class, "walla-button")]',
            '//span[text()="Cargar más"]/ancestor::button',
            '//div[contains(@class, "loadMore")]//button'
        ]
        
        # Probar selectores CSS
        for selector in button_selectors:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                print(f" Selector CSS '{selector}': {len(buttons)} elementos")
                
                for button in buttons:
                    try:
                        if button.is_displayed() and button.is_enabled():
                            # Scroll al botón
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                            time.sleep(1)
                            
                            # Intentar clic
                            try:
                                button.click()
                                print(" CLIC EXITOSO en 'Cargar más' (CSS)")
                                return True
                            except:
                                driver.execute_script("arguments[0].click();", button)
                                print(" CLIC JS EXITOSO en 'Cargar más'")
                                return True
                    except Exception as btn_error:
                        print(f" Error con botón: {btn_error}")
                        continue
            except Exception as selector_error:
                print(f" Selector CSS falló: {selector_error}")
                continue
        
        # Probar selectores XPath
        for xpath in xpath_selectors:
            try:
                buttons = driver.find_elements(By.XPATH, xpath)
                print(f" Selector XPath '{xpath}': {len(buttons)} elementos")
                
                for button in buttons:
                    try:
                        if button.is_displayed() and button.is_enabled():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                            time.sleep(1)
                            
                            try:
                                button.click()
                                print(" CLIC EXITOSO en 'Cargar más' (XPath)")
                                return True
                            except:
                                driver.execute_script("arguments[0].click();", button)
                                print(" CLIC JS EXITOSO en 'Cargar más'")
                                return True
                    except:
                        continue
            except:
                continue
        
        print(" NO SE ENCONTRÓ BOTÓN 'Cargar más' VÁLIDO")
        return False
        
    except Exception as e:
        print(f" ERROR BUSCANDO BOTÓN: {e}")
        return False

def smart_scroll_and_load_FIXED(driver, max_clicks=2):
    """SCROLL INTELIGENTE CORREGIDO con botón funcional"""
    print(" Iniciando scroll inteligente CORREGIDO...")
    
    # Contar enlaces iniciales
    initial_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    print(f" Enlaces iniciales: {initial_count}")
    
    # FASE 1: Scroll inicial suave
    print(" Fase 1: Scroll inicial...")
    for i in range(10):
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.3)
    
    current_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    print(f" Tras scroll inicial: {current_count}")
    
    # FASE 2: Hacer clic en "Cargar más"
    clicks_made = 0
    for click_attempt in range(max_clicks):
        print(f" Intento de clic {click_attempt + 1}/{max_clicks}...")
        
        # Scroll hasta el final
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        count_before = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
        
        if click_load_more_button_FIXED(driver):
            clicks_made += 1
            time.sleep(3)  # Esperar a que cargue
            
            count_after = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
            gained = count_after - count_before
            
            print(f" Clic {clicks_made}: {count_before} → {count_after} (+{gained})")
            
            if gained > 0:
                # Scroll después del clic para revelar nuevos elementos
                for scroll in range(5):
                    driver.execute_script("window.scrollBy(0, 1000);")
                    time.sleep(0.5)
            else:
                print(" El clic no agregó elementos nuevos")
                break
        else:
            print(" No se pudo hacer clic, fin de contenido")
            break
    
    # FASE 3: Scroll intensivo final (150 scrolls como solicita)
    print(" Fase 3: Scroll intensivo de 150 iteraciones...")
    
    for scroll_num in range(150):
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.2)
        
        # Cada 25 scrolls, mostrar progreso
        if scroll_num % 25 == 0 and scroll_num > 0:
            current = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
            print(f" Scroll {scroll_num}/150: {current} enlaces")
        
        # Cada 50 scrolls, scroll hasta el final para activar más contenido
        if scroll_num % 50 == 0:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
    
    # Conteo final
    final_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    total_gained = final_count - initial_count
    
    print(f" RESUMEN DEL SCROLL:")
    print(f"   Inicial: {initial_count}")
    print(f"   Final: {final_count}")
    print(f"   Ganados: +{total_gained}")
    print(f"   Clics realizados: {clicks_made}")
    print(f"   Scrolls realizados: 150")
    
    return final_count

def validate_snippet_cb125r_FIXED(ad_element):
    """VALIDAR SNIPPET HONDA CB125R con extractores CORREGIDOS"""
    try:
        # Extraer título y precio con selectores corregidos
        title = extract_title_from_snippet_FIXED(ad_element)
        price_text = extract_price_from_snippet_FIXED(ad_element)
        
        print(f" VALIDANDO: Título='{title}', Precio='{price_text}'")
        
        # Validar título para CB125R
        if not title or len(title) < 3:
            print(" RECHAZADO: Título vacío o muy corto")
            return False, None, None
        
        title_lower = title.lower()
        
        # Verificar que menciona Honda y CB125R
        honda_keywords = ['honda', 'handa']  # Incluir error común
        cb125r_keywords = ['cb125r', 'cb 125 r', 'cb 125r', 'cb-125-r']
        
        has_honda = any(keyword in title_lower for keyword in honda_keywords)
        has_cb125r = any(keyword in title_lower for keyword in cb125r_keywords)
        
        if not (has_honda or has_cb125r):
            print(" RECHAZADO: No menciona Honda CB125R")
            return False, None, None
        
        # Extraer valor numérico del precio
        price_value = None
        if price_text:
            # Extraer números del precio
            price_numbers = re.findall(r'\d+', price_text.replace('.', '').replace(',', ''))
            if price_numbers:
                price_value = int(''.join(price_numbers))
                
                # Validar rango de precio (1500-5000€)
                if not (1500 <= price_value <= 5000):
                    print(f" RECHAZADO: Precio {price_value}€ fuera de rango 1500-5000€")
                    return False, None, None
        
        print(f" VÁLIDO: Honda CB125R - {title} - {price_text}")
        return True, title, price_value
        
    except Exception as e:
        print(f" ERROR VALIDANDO SNIPPET: {e}")
        return False, None, None

def scrape_cb125r_ULTRA_OPTIMIZADO(driver, model_info):
    """SCRAPER CB125R ULTRA OPTIMIZADO Y CORREGIDO"""
    
    print(" INICIANDO SCRAPER CB125R ULTRA OPTIMIZADO")
    print("="*60)
    
    modelo = model_info["modelo"]
    min_price = model_info["min_price"]
    max_price = model_info["max_price"]
    
    print(f" Modelo: {modelo}")
    print(f" Precio: {min_price}€ - {max_price}€")
    
    # Obtener URLs optimizadas
    search_urls = get_search_urls_optimized(modelo, min_price, max_price)
    
    all_valid_links = set()
    cookies_accepted = False
    
    print(f"\n FASE 1: RECOPILACIÓN INTELIGENTE")
    print(f"URLs a procesar: {len(search_urls)}")
    
    # Procesar cada URL
    for idx, url in enumerate(tqdm(search_urls[:10], desc="Procesando URLs", colour="green")):  # Limitar a 10 para prueba
        try:
            print(f"\n URL {idx+1}/{len(search_urls[:10])}: {url[:80]}...")
            
            # Cargar página
            driver.get(url)
            time.sleep(3)
            
            # Aceptar cookies solo una vez
            if not cookies_accepted:
                try:
                    cookie_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Aceptar') or contains(text(), 'Acepto')]"))
                    )
                    cookie_button.click()
                    cookies_accepted = True
                    time.sleep(2)
                    print(" Cookies aceptadas")
                except:
                    print(" No se encontraron cookies")
            
            # SCROLL INTELIGENTE CORREGIDO
            print(" Realizando scroll inteligente...")
            total_links = smart_scroll_and_load_FIXED(driver, max_clicks=2)
            print(f" Total enlaces después del scroll: {total_links}")
            
            # BUSCAR Y VALIDAR SNIPPETS
            print(" Buscando anuncios Honda CB125R...")
            
            # Selectores actualizados para contenedores de anuncios
            ad_selectors = [
                'a[href*="/item/"][class*="item-card_ItemCard"]',  # Selector específico 2025
                'a[href*="/item/"]',                               # Genérico
                '*[class*="ItemCard"] a[href*="/item/"]'          # Alternativo
            ]
            
            ad_containers = []
            for selector in ad_selectors:
                try:
                    containers = driver.find_elements(By.CSS_SELECTOR, selector)
                    if containers:
                        ad_containers = containers
                        print(f" Encontrados {len(containers)} anuncios con selector: {selector}")
                        break
                except:
                    continue
            
            if not ad_containers:
                print(" No se encontraron contenedores de anuncios")
                continue
            
            # Validar cada snippet
            valid_count_this_url = 0
            for container in ad_containers:
                try:
                    href = container.get_attribute('href')
                    if not href or href in all_valid_links:
                        continue
                    
                    # VALIDAR SNIPPET CON EXTRACTORES CORREGIDOS
                    is_valid, title, price = validate_snippet_cb125r_FIXED(container)
                    
                    if is_valid:
                        all_valid_links.add(href)
                        valid_count_this_url += 1
                        print(f" VÁLIDO #{len(all_valid_links)}: {title}")
                    
                except Exception as e:
                    continue
            
            print(f" URL {idx+1} completada: {valid_count_this_url} anuncios CB125R válidos")
            print(f" Total acumulado: {len(all_valid_links)} anuncios CB125R")
            
            # Límite de seguridad
            if len(all_valid_links) >= 500:
                print(" Límite de 500 anuncios alcanzado")
                break
                
        except Exception as e:
            print(f" Error en URL {idx+1}: {e}")
            continue
    
    print(f"\n FASE 1 COMPLETADA")
    print(f" Total anuncios Honda CB125R encontrados: {len(all_valid_links)}")
    
    if not all_valid_links:
        print(" No se encontraron anuncios válidos")
        return []
    
    # FASE 2: ANÁLISIS DETALLADO (limitado para prueba)
    print(f"\n FASE 2: ANÁLISIS DETALLADO")
    results = []
    
    # Analizar primeros 20 para prueba
    sample_links = list(all_valid_links)[:20]
    
    for idx, url in enumerate(tqdm(sample_links, desc="Análisis detallado", colour="blue")):
        try:
            print(f"\n Analizando {idx+1}/{len(sample_links)}: {url}")
            
            # Cargar página del anuncio
            driver.get(url)
            time.sleep(2)
            
            # EXTRAER DATOS CON SELECTORES ACTUALIZADOS
            data = extract_detailed_data_FIXED(driver, url)
            
            if data:
                results.append(data)
                print(f" Datos extraídos: {data.get('Titulo', 'Sin título')}")
            else:
                print(" No se pudieron extraer datos")
                
        except Exception as e:
            print(f" Error en análisis detallado: {e}")
            continue
    
    print(f"\n SCRAPING COMPLETADO")
    print(f" Anuncios Honda CB125R procesados: {len(results)}")
    
    return results

def extract_detailed_data_FIXED(driver, url):
    """EXTRAER DATOS DETALLADOS con selectores ACTUALIZADOS"""
    try:
        data = {
            'URL': url,
            'Fecha_Extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # TÍTULO - Selectores actualizados
        title_selectors = [
            'h1[class*="ItemDetailTwoColumns__title"]',  # Selector específico
            'h1[class*="title"]',
            'h1',
            '*[data-testid="item-title"]'
        ]
        
        title = extract_with_selectors(driver, title_selectors)
        data['Titulo'] = title or "No disponible"
        
        # PRECIO - Selectores actualizados  
        price_selectors = [
            'span[class*="ItemDetailPrice--standard"]',
            '*[class*="price"]',
            '*[aria-label*="Item Price"]'
        ]
        
        price = extract_with_selectors(driver, price_selectors)
        data['Precio'] = price or "No disponible"
        
        # DESCRIPCIÓN
        desc_selectors = [
            'section[class*="description"]',
            '*[class*="description"]'
        ]
        
        description = extract_with_selectors(driver, desc_selectors)
        data['Descripcion'] = description or "No disponible"
        
        # UBICACIÓN
        location_selectors = [
            'a[href*="/"][title*="Segunda mano"]',
            '*[class*="location"] a',
            '*[data-testid="location"]'
        ]
        
        location = extract_with_selectors(driver, location_selectors)
        data['Ubicacion'] = location or "No disponible"
        
        # VENDEDOR
        seller_selectors = [
            'h3[class*="ItemDetailSellerInformation__text--typoMidM"]',
            'h3[class*="seller"]'
        ]
        
        seller = extract_with_selectors(driver, seller_selectors)
        data['Vendedor'] = seller or "No disponible"
        
        return data
        
    except Exception as e:
        print(f" Error extrayendo datos detallados: {e}")
        return None

def extract_with_selectors(driver, selectors):
    """EXTRAER TEXTO usando lista de selectores"""
    for selector in selectors:
        try:
            element = driver.find_element(By.CSS_SELECTOR, selector)
            text = element.text.strip()
            if text:
                return text
        except:
            continue
    return None

def main():
    """FUNCIÓN PRINCIPAL CORREGIDA"""
    
    print(" HONDA CB125R SCRAPER ULTRA CORREGIDO v2.5")
    print("="*60)
    
    try:
        # Configurar navegador
        print(" Configurando navegador Chrome...")
        driver = setup_browser()
        
        # Configuración del modelo
        model_info = {
            "modelo": "CB125R", 
            "min_price": 1500, 
            "max_price": 5000
        }
        
        print(f"🎯 Configuración: {model_info}")
        
        # Ejecutar scraping corregido
        results = scrape_cb125r_ULTRA_OPTIMIZADO(driver, model_info)
        
        if results:
            # Generar archivo Excel
            print(f"\n Generando archivo Excel con {len(results)} resultados...")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = f"honda_cb125r_CORREGIDO_{timestamp}.xlsx"
            
            df = pd.DataFrame(results)
            df.to_excel(filename, index=False)
            
            print(f" Archivo generado: {filename}")
            print(f" Columnas incluidas: {list(df.columns)}")
            
        else:
            print(" No se obtuvieron resultados")
            
    except Exception as e:
        print(f" ERROR CRÍTICO: {e}")
        
    finally:
        if 'driver' in locals():
            try:
                driver.quit()
                print("🔚 Navegador cerrado")
            except:
                pass

if __name__ == "__main__":
    main()
