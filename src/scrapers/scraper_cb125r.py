 """
================================================================================
                    SCRAPER HONDA CB125R ACTUALIZADO - WALLAPOP MOTOS SCRAPER                    
================================================================================

Scraper específico actualizado para Honda CB125R
Con selectores reales de Wallapop y extracción robusta optimizada para GitHub Actions

Características:
- Detectión específica de Honda CB125R y variantes
- Filtrado optimizado con selectores reales
- URLs optimizadas para este modelo específico
- Validación estricta de marca y modelo
- Timeouts optimizados para GitHub Actions

Autor: Carlos Peraza
Versión: 1.1 - Actualizada para Wallapop actual
Fecha: Septiembre 2025
================================================================================
"""

import re
import logging
from typing import Dict, List, Optional
from scrapers.base_scraper import BaseScraper

class ScraperCB125R(BaseScraper):
    """Scraper específico actualizado para Honda CB125R"""
    
    def __init__(self):
        super().__init__('cb125r')
        self.logger = logging.getLogger(__name__)
        
        # Configuración específica del modelo
        self.model_patterns = [
            r'\bcb[\-\s]*125[\-\s]*r\b',
            r'\bcb125r\b',
            r'\bcb\s*125\s*r\b',
            r'\bcb\s*125[\-\.\/]\s*r\b',
            r'\bc[\-\s]*b[\-\s]*125[\-\s]*r\b'
        ]
        
        self.exclude_patterns = [
            r'\bcb[\-\s]*125[\-\s]*f\b',  # CB125F
            r'\bcb[\-\s]*250[\-\s]*r\b',  # CB250R
            r'\bcb[\-\s]*500[\-\s]*r\b',  # CB500R
            r'\bcb[\-\s]*650[\-\s]*r\b',  # CB650R
            r'\bcbr[\-\s]*125\b',         # CBR125 (diferente modelo)
            r'\bcbr\b',                   # CBR en general
            r'\bcb[\-\s]*600\b',          # CB600
            r'\bcb[\-\s]*1000\b'          # CB1000
        ]
        
        self.logger.info(f"Scraper CB125R inicializado - Precio: {self.modelo_config['precio_min']}€-{self.modelo_config['precio_max']}€")
    
    def get_search_urls(self) -> List[str]:
        """Generar URLs de búsqueda optimizadas para Honda CB125R"""
        min_price = self.modelo_config['precio_min']
        max_price = self.modelo_config['precio_max']
        
        urls = []
        
        # URLs principales más efectivas para CB125R (reducidas para GitHub Actions)
        base_queries = [
            "honda%20cb125r",
            "honda%20cb%20125%20r", 
            "cb125r",
            "cb%20125%20r",
            "honda%20cb125r%202020",
            "honda%20cb125r%202021",
            "honda%20cb125r%202022",
            "honda%20cb125r%202023",
            "honda%20cb125r%202024"
        ]
        
        # Generar URLs con filtros de precio (solo ordenamiento básico)
        for query in base_queries:
            # URL básica con filtro de precio
            urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}")
            
            # URL ordenada por más recientes (solo para queries principales)
            if query in ["honda%20cb125r", "cb125r"]:
                urls.append(f"https://es.wallapop.com/app/search?keywords={query}&min_sale_price={min_price}&max_sale_price={max_price}&order_by=newest")
        
        # Eliminar duplicados manteniendo orden
        unique_urls = []
        seen = set()
        for url in urls:
            if url not in seen:
                unique_urls.append(url)
                seen.add(url)
        
        # Limitar URLs para GitHub Actions
        max_urls = 8 if self._is_github_actions() else len(unique_urls)
        final_urls = unique_urls[:max_urls]
        
        self.logger.info(f"{len(final_urls)} URLs generadas para CB125R")
        return final_urls
    
    def _is_github_actions(self) -> bool:
        """Detectar si se ejecuta en GitHub Actions"""
        import os
        return os.getenv("GITHUB_ACTIONS") == "true"
    
    def validate_moto_data(self, moto_data: Dict) -> bool:
        """
        Validar si los datos corresponden realmente a una Honda CB125R
        
        Args:
            moto_data: Diccionario con datos de la moto
            
        Returns:
            bool: True si es válida CB125R
        """
        try:
            titulo = moto_data.get('Título', '').lower()
            descripcion = moto_data.get('Descripcion', '').lower()
            combined_text = f"{titulo} {descripcion}"
            
            # PASO 1: Verificar que sea Honda
            honda_found = self._is_honda_brand(combined_text)
            if not honda_found:
                self.logger.debug("No es Honda")
                return False
            
            # PASO 2: Verificar modelo CB125R específico
            cb125r_found = self._is_cb125r_model(combined_text)
            if not cb125r_found:
                self.logger.debug("No es CB125R")
                return False
            
            # PASO 3: Excluir otros modelos CB
            if self._is_excluded_model(combined_text):
                self.logger.debug("Es otro modelo CB excluido")
                return False
            
            # PASO 4: Validar precio si está disponible
            if not self._is_valid_price_range(moto_data.get('Precio', '')):
                self.logger.debug("Precio fuera de rango")
                return False
            
            # PASO 5: Validar año si está disponible
            if not self._is_valid_year_range(moto_data.get('Año', '')):
                self.logger.debug("Año fuera de rango")
                return False
            
            self.logger.debug(f"CB125R válida: {titulo[:50]}")
            return True
            
        except Exception as e:
            self.logger.warning(f"Error validando moto: {e}")
            return False
    
    def _is_honda_brand(self, text: str) -> bool:
        """Verificar si es marca Honda"""
        honda_patterns = [
            r'\bhonda\b',
            r'\bhond\b',  # Error común
        ]
        
        for pattern in honda_patterns:
            if re.search(pattern, text):
                return True
        
        # Si encuentra CB125R específico, asumir que es Honda
        cb125r_specific = [
            r'\bcb[\-\s]*125[\-\s]*r\b',
            r'\bcb125r\b'
        ]
        
        for pattern in cb125r_specific:
            if re.search(pattern, text):
                return True
        
        return False
    
    def _is_cb125r_model(self, text: str) -> bool:
        """Verificar si es modelo CB125R específico"""
        for pattern in self.model_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_excluded_model(self, text: str) -> bool:
        """Verificar si es un modelo excluido (CB125F, CB250R, etc.)"""
        for pattern in self.exclude_patterns:
            if re.search(pattern, text):
                return True
        return False
    
    def _is_valid_price_range(self, precio_text: str) -> bool:
        """Validar si el precio está en el rango esperado para CB125R"""
        if not precio_text or precio_text == "No especificado":
            return True  # Aceptar si no hay precio
        
        try:
            # Extraer números del precio
            price_numbers = re.findall(r'\d+', precio_text.replace('.', '').replace(',', '').replace(' ', ''))
            if price_numbers:
                price = int(''.join(price_numbers))
                min_price = self.modelo_config['precio_min']
                max_price = self.modelo_config['precio_max']
                
                # Rango específico para CB125R: 1000€ - 4500€
                return min_price <= price <= max_price
        except:
            pass
        
        return True  # Aceptar si no se puede parsear
    
    def _is_valid_year_range(self, año_text: str) -> bool:
        """Validar si el año está en el rango esperado para CB125R"""
        if not año_text or año_text == "No especificado":
            return True  # Aceptar si no hay año
        
        try:
            year_match = re.search(r'(20[0-9]{2})', str(año_text))
            if year_match:
                year = int(year_match.group(1))
                min_year = self.modelo_config['año_min']
                max_year = self.modelo_config['año_max']
                
                # Rango específico para CB125R: 2018-2025
                return min_year <= year <= max_year
        except:
            pass
        
        return True  # Aceptar si no se puede parsear
    
    # Métodos de extracción específicos para CB125R (heredan de base_scraper actualizado)
    
    def extract_titulo(self) -> str:
        """Extraer título específico para CB125R usando método actualizado"""
        return self.extract_titulo_wallapop()
    
    def extract_precio(self) -> str:
        """Extraer precio específico para CB125R usando método actualizado"""
        return self.extract_precio_wallapop()
    
    def extract_kilometraje(self) -> str:
        """Extraer kilometraje usando función robusta actualizada"""
        return self.extract_kilometraje_wallapop()
    
    def extract_año(self) -> str:
        """Extraer año usando función robusta actualizada"""
        return self.extract_año_wallapop()
    
    def extract_vendedor(self) -> str:
        """Extraer vendedor con detección de comerciales específicos"""
        vendedor = self.extract_vendedor_wallapop()
        
        # Detectar si es comercial (específico para CB125R)
        if vendedor and vendedor != "Particular":
            commercial_keywords = ['concesionario', 'motor', 'moto', 'honda', 'taller', 'cb125r', 'cb', '125']
            if any(word in vendedor.lower() for word in commercial_keywords):
                return f" {vendedor}"
        
        return vendedor
    
    def extract_ubicacion(self) -> str:
        """Extraer ubicación usando método actualizado"""
        return self.extract_ubicacion_wallapop()
    
    def extract_fecha_publicacion(self) -> str:
        """Extraer fecha usando método actualizado"""
        return self.extract_fecha_publicacion_wallapop()
    
    def extract_descripcion(self) -> str:
        """Extraer descripción usando método actualizado"""
        return self.extract_descripcion_wallapop()

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def run_cb125r_scraper():
    """Función principal para ejecutar el scraper actualizado de CB125R"""
    try:
        print(" Iniciando scraper Honda CB125R actualizado...")
        
        scraper = ScraperCB125R()
        df_results = scraper.scrape_model()
        
        if not df_results.empty:
            print(f" Scraping CB125R completado: {len(df_results)} motos encontradas")
            
            # Mostrar estadísticas de calidad
            total = len(df_results)
            precios_ok = len(df_results[df_results['Precio'] != 'No especificado'])
            km_ok = len(df_results[df_results['Kilometraje'] != 'No especificado'])
            años_ok = len(df_results[df_results['Año'] != 'No especificado'])
            
            print(f" Calidad de extracción:")
            print(f"   • Precios: {precios_ok}/{total} ({precios_ok/total*100:.1f}%)")
            print(f"   • Kilometraje: {km_ok}/{total} ({km_ok/total*100:.1f}%)")
            print(f"   • Años: {años_ok}/{total} ({años_ok/total*100:.1f}%)")
            
            return df_results
        else:
            print(" No se encontraron motos CB125R")
            return df_results
            
    except Exception as e:
        print(f" Error en scraper CB125R: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Ejecutar scraper directamente
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    results = run_cb125r_scraper()
    if results is not None and not results.empty:
        print(f"\n Primeras 3 motos encontradas:")
        for i, (_, moto) in enumerate(results.head(3).iterrows()):
            print(f"   {i+1}. {moto['Título'][:40]}...")
            print(f"       {moto['Precio']} |  {moto['Ubicación']} |  {moto['Año']}")
