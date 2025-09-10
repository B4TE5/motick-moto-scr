"""
================================================================================
                          SCRAPER CB125R CORREGIDO                     
================================================================================

Autor: Carlos Peraza
Version: 3.0 
Fecha: Septiembre 2025

CORRECCIONES:
- URLs optimizadas para encontrar mas CB125R
- Validacion mejorada del modelo
- Hereda de BaseScraper corregido

================================================================================
"""

import re
import logging
from typing import Dict, List, Optional
from scrapers.base_scraper import BaseScraper

class ScraperCB125R(BaseScraper):
    """Scraper especifico para Honda CB125R CORREGIDO"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Configuracion especifica del modelo
        self.model_patterns = [
            r'\bcb[\-\s]*125[\-\s]*r\b',
            r'\bcb125r\b',
            r'\bcb\s*125\s*r\b',
            r'\bcb\s*125[\-\.\/]\s*r\b',
            r'\bhonda\s+cb\s*125\s*r\b'
        ]
        
        self.exclude_patterns = [
            r'\bcb[\-\s]*125[\-\s]*f\b',  # CB125F
            r'\bcb[\-\s]*250[\-\s]*r\b',  # CB250R
            r'\bcb[\-\s]*500[\-\s]*r\b',  # CB500R
            r'\bcb[\-\s]*650[\-\s]*r\b',  # CB650R
            r'\bcbr[\-\s]*125\b',         # CBR125
            r'\bcbr\b'                    # CBR en general
        ]
        
        self.logger.info(f"Scraper CB125R inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """CORREGIDO: URLs optimizadas para encontrar mas CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # CONSULTAS PRINCIPALES OPTIMIZADAS
        main_queries = [
            "honda%20cb125r",
            "honda%20cb%20125%20r", 
            "cb125r",
            "cb%20125%20r",
            "honda%20cb125r%20moto",
            "moto%20honda%20cb125r"
        ]
        
        # CONSULTAS POR AÑOS (2018-2024)
        year_queries = []
        for year in range(2018, 2025):
            year_queries.extend([
                f"honda%20cb125r%20{year}",
                f"cb125r%20{year}",
                f"honda%20cb%20125%20r%20{year}"
            ])
        
        # CONSULTAS CON CARACTERISTICAS
        feature_queries = [
            "honda%20cb125r%20deportiva",
            "honda%20cb125r%20naked",
            "honda%20cb125r%20abs",
            "cb125r%20sport",
            "honda%20125%20deportiva",
            "moto%20deportiva%20125%20honda"
        ]
        
        # CONSULTAS DE MERCADO
        market_queries = [
            "honda%20cb125r%20segunda%20mano",
            "honda%20cb125r%20usado",
            "honda%20cb125r%20ocasion",
            "cb125r%20particular",
            "honda%20cb125r%20seminuevo"
        ]
        
        # CONSULTAS CON ERRORES COMUNES
        typo_queries = [
            "honda%20cb%20125r",
            "honda%20cb125%20r",
            "hond%20cb125r",
            "honda%20cb125",
            "honda%20125%20cb"
        ]
        
        # COMBINAR TODAS LAS CONSULTAS
        all_queries = main_queries + year_queries + feature_queries + market_queries + typo_queries
        
        # ORDENAMIENTOS
        orderings = [
            "",  # Sin ordenamiento
            "&order_by=newest",
            "&order_by=price_low_to_high", 
            "&order_by=price_high_to_low"
        ]
        
        # GENERAR URLs CON PRECIO
        for query in all_queries:
            for ordering in orderings:
                url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}{ordering}"
                urls.append(url)
        
        # BUSQUEDAS POR REGIONES PRINCIPALES
        main_regions = [
            ("madrid", "40.4168", "-3.7038"),
            ("barcelona", "41.3851", "2.1734"),
            ("valencia", "39.4699", "-0.3763"),
            ("sevilla", "37.3891", "-5.9845"),
            ("bilbao", "43.2627", "-2.9253")
        ]
        
        region_queries = ["honda%20cb125r", "cb125r"]
        
        for region, lat, lon in main_regions:
            for query in region_queries:
                for ordering in orderings[:2]:  # Solo 2 ordenamientos por region
                    url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&latitude={lat}&longitude={lon}&distance=50000{ordering}"
                    urls.append(url)
        
        # BUSQUEDAS POR RANGOS DE PRECIOS
        price_ranges = [
            (min_price, min_price + 800),
            (min_price + 600, min_price + 1500), 
            (min_price + 1200, max_price)
        ]
        
        for query in ["honda%20cb125r", "cb125r"]:
            for min_p, max_p in price_ranges:
                url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_p}&max_sale_price={max_p}"
                urls.append(url)
        
        # BUSQUEDAS SIN FILTROS DE PRECIO (para encontrar gangas)
        no_price_queries = [
            "honda%20cb125r%20ganga",
            "honda%20cb125r%20urge",
            "cb125r%20barato"
        ]
        
        for query in no_price_queries:
            url = f"https://es.wallapop.com/app/search?keywords={query}"
            urls.append(url)
        
        # ELIMINAR DUPLICADOS
        unique_urls = list(dict.fromkeys(urls))
        
        self.logger.info(f"{len(unique_urls)} URLs generadas para CB125R")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """CORREGIDO: Validacion mejorada para Honda CB125R"""
        try:
            titulo = moto_data.get('Titulo', '').lower()
            
            if not titulo or titulo == "sin titulo":
                # Si no hay titulo, validar por precio
                precio_text = moto_data.get('Precio', '')
                if precio_text and precio_text != "No especificado":
                    try:
                        price_numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', ''))
                        if price_numbers:
                            price = int(price_numbers[0])
                            min_price = self.modelo_config['precio_min']
                            max_price = self.modelo_config['precio_max']
                            
                            if min_price <= price <= max_price:
                                return True
                    except:
                        pass
                return False
            
            # VERIFICAR QUE SEA HONDA CB125R
            honda_found = any(word in titulo for word in ['honda', 'hond'])
            cb125r_patterns = ['cb125r', 'cb 125 r', 'cb-125-r', 'cb 125r', 'cb125 r']
            cb125r_found = any(pattern in titulo for pattern in cb125r_patterns)
            
            # Excluir otros modelos CB
            excluded_models = ['cb125f', 'cb250r', 'cb500r', 'cbr125', 'cbr', 'cb125 f']
            excluded = any(model in titulo for model in excluded_models)
            
            if excluded:
                self.logger.debug(f"Excluido por ser otro modelo CB: {titulo}")
                return False
            
            # Si encuentra CB125R, es valido
            if cb125r_found:
                return True
            
            # Si solo encuentra Honda en rango de precio, puede ser valido
            if honda_found:
                precio_text = moto_data.get('Precio', '')
                if precio_text and precio_text != "No especificado":
                    try:
                        price_numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', ''))
                        if price_numbers:
                            price = int(price_numbers[0])
                            min_price = self.modelo_config['precio_min']
                            max_price = self.modelo_config['precio_max']
                            
                            # Rango mas estricto para validacion sin modelo explicito
                            if min_price + 200 <= price <= max_price - 200:
                                return True
                    except:
                        pass
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False

# FUNCION PRINCIPAL
def run_cb125r_scraper():
    """Funcion principal para ejecutar el scraper de CB125R CORREGIDO"""
    try:
        scraper = ScraperCB125R()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f"Scraping CB125R completado: {len(df_results)} motos encontradas")
            return df_results
        else:
            print("No se encontraron motos CB125R")
            return df_results
            
    except Exception as e:
        print(f"Error en scraper CB125R: {e}")
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_cb125r_scraper()
    if results is not None and not results.empty:
        print(f"\nPrimeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Titulo']} - {moto['Precio']} - {moto['Año']}")
