"""
================================================================================
                                 SCRAPER CB125R                     
================================================================================

Autor: Carlos Peraza
Versión: 2.0 
Fecha: Agosto 2025

================================================================================
"""

import re
import logging
from typing import Dict, List, Optional
from scrapers.base_scraper import BaseScraper

class ScraperCB125R(BaseScraper):
    """Scraper específico para Honda CB125R con URLs extendidas"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bcb[\-\s]*125[\-\s]*r\b',
            r'\bcb125r\b',
            r'\bcb\s*125\s*r\b',
            r'\bcb\s*125[\-\.\/]\s*r\b'
        ]
        
        self.exclude_patterns = [
            r'\bcb[\-\s]*125[\-\s]*f\b',  # CB125F
            r'\bcb[\-\s]*250[\-\s]*r\b',  # CB250R
            r'\bcb[\-\s]*500[\-\s]*r\b',  # CB500R
            r'\bcb[\-\s]*650[\-\s]*r\b',  # CB650R
            r'\bcbr[\-\s]*125\b',         # CBR125 (diferente modelo)
            r'\bcbr\b'                    # CBR en general
        ]
        
        self.logger.info(f"Scraper CB125R inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """CORREGIDO: Generar MUCHAS MAS URLs de búsqueda para CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # TERMINOS DE BUSQUEDA PRINCIPALES EXPANDIDOS
        base_queries = [
            # Búsquedas principales
            "honda%20cb125r",
            "honda%20cb%20125%20r", 
            "cb125r",
            "cb%20125%20r",
            "honda%20cb125r%20moto",
            "moto%20honda%20cb125r",
            "cb125r%20honda",
            "honda%20cb%20125%20deportiva",
            
            # Por años específicos (más URLs)
            "honda%20cb125r%202018",
            "honda%20cb125r%202019",
            "honda%20cb125r%202020",
            "honda%20cb125r%202021",
            "honda%20cb125r%202022",
            "honda%20cb125r%202023",
            "honda%20cb125r%202024",
            "honda%20cb125r%202025",
            
            # Combinaciones con características
            "cb125r%20deportiva",
            "cb125r%20naked",
            "cb125r%20sport",
            "honda%20cb125r%20abs",
            "honda%20cb125r%20led",
            "moto%20deportiva%20125%20honda",
            "moto%20125%20honda%20cb",
            
            # Términos alternativos
            "honda%20cb%20125%20sport",
            "honda%20125%20deportiva",
            "honda%20naked%20125",
            
            # Búsquedas por rangos de precios
            "honda%20cb125r%20barata",
            "honda%20cb125r%20segunda%20mano",
            "honda%20cb125r%20usada",
            "honda%20cb125r%20ocasion",
            "cb125r%20oferta",
            "cb125r%20precio",
            
            # Búsquedas por estado
            "honda%20cb125r%20seminueva",
            "honda%20cb125r%20como%20nueva",
            "honda%20cb125r%20pocos%20km",
            "honda%20cb125r%20poco%20uso",
            
            # Con errores ortográficos comunes
            "honda%20cb125%20r",
            "honda%20cb%20125r",
            "hond%20cb125r",
            "honda%20cb125",
            "honda%20125%20cb"
        ]
        
        # ORDENAMIENTOS PARA CADA BUSQUEDA
        orderings = [
            "",  # Sin ordenamiento
            "&order_by=newest",
            "&order_by=price_low_to_high", 
            "&order_by=price_high_to_low",
            "&order_by=closest"
        ]
        
        # GENERAR URLs CON TODOS LOS ORDENAMIENTOS
        for query in base_queries:
            for ordering in orderings:
                # URL básica con filtro de precio
                url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}{ordering}"
                urls.append(url)
        
        # BUSQUEDAS POR REGIONES PRINCIPALES (MAS REGIONES)
        main_regions = [
            ("madrid", "40.4168", "-3.7038"),
            ("barcelona", "41.3851", "2.1734"),
            ("valencia", "39.4699", "-0.3763"),
            ("sevilla", "37.3891", "-5.9845"),
            ("bilbao", "43.2627", "-2.9253"),
            ("zaragoza", "41.6488", "-0.8891"),
            ("malaga", "36.7196", "-4.4214"),
            ("murcia", "37.9922", "-1.1307"),
            ("palma", "39.5696", "2.6502"),
            ("las%20palmas", "28.1248", "-15.4300")
        ]
        
        main_queries = ["honda%20cb125r", "cb125r", "honda%20cb%20125"]
        
        for region, lat, lon in main_regions:
            for query in main_queries:
                for ordering in orderings[:3]:  # Solo 3 ordenamientos por región
                    url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&latitude={lat}&longitude={lon}&distance=50000{ordering}"
                    urls.append(url)
        
        # BUSQUEDAS POR RANGOS DE PRECIOS ESPECIFICOS
        price_ranges = [
            (min_price, min_price + 500),
            (min_price + 500, min_price + 1000), 
            (min_price + 1000, min_price + 1500),
            (min_price + 1500, max_price)
        ]
        
        main_price_queries = ["honda%20cb125r", "cb125r"]
        
        for query in main_price_queries:
            for min_p, max_p in price_ranges:
                url = f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_p}&max_sale_price={max_p}"
                urls.append(url)
        
        # BUSQUEDAS SIN FILTROS DE PRECIO (para encontrar gangas)
        no_price_queries = [
            "honda%20cb125r%20ganga",
            "honda%20cb125r%20regalo", 
            "honda%20cb125r%20urge",
            "cb125r%20barata"
        ]
        
        for query in no_price_queries:
            url = f"https://es.wallapop.com/app/search?keywords={query}"
            urls.append(url)
        
        # ELIMINAR DUPLICADOS MANTENIENDO ORDEN
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        self.logger.info(f"{len(unique_urls)} URLs generadas para CB125R (vs ~25 originales)")
        return unique_urls
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """Validar si los datos corresponden realmente a una Honda CB125R"""
        try:
            titulo = moto_data.get('Título', '').lower()
            
            # VALIDACION ESTRICTA PERO FLEXIBLE
            if not titulo or titulo == "sin título":
                # Si no hay título, validar por precio al menos
                precio_text = moto_data.get('Precio', '')
                if precio_text and precio_text != "No especificado":
                    try:
                        price_numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', ''))
                        if price_numbers:
                            price = int(price_numbers[0])
                            min_price = self.modelo_config['precio_min']
                            max_price = self.modelo_config['precio_max']
                            
                            # Si el precio está en rango, aceptar (puede ser CB125R)
                            if min_price <= price <= max_price:
                                return True
                    except:
                        pass
                return False
            
            # VERIFICAR QUE SEA HONDA CB125R
            honda_found = any(word in titulo for word in ['honda', 'hond'])
            cb125r_found = any(pattern in titulo for pattern in ['cb125r', 'cb 125 r', 'cb-125-r', 'cb 125r'])
            
            # Excluir otros modelos
            excluded = any(word in titulo for word in ['cb125f', 'cb250r', 'cb500r', 'cbr125', 'cbr'])
            
            if excluded:
                self.logger.debug(f"Excluido por ser otro modelo CB: {titulo}")
                return False
            
            # Si encuentra CB125R (con o sin Honda), es válido
            if cb125r_found:
                return True
            
            # Si solo encuentra Honda pero está en rango de precio, puede ser válido
            if honda_found:
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
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False

# ============================================================================
# FUNCION PRINCIPAL
# ============================================================================

def run_cb125r_scraper():
    """Función principal para ejecutar el scraper de CB125R"""
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
            print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Año']}")
