"""
================================================================================
                    LIMPIADOR INTEGRADO - WALLAPOP MOTOS SCRAPER                    
================================================================================

Sistema de limpieza integrado que aplica automáticamente los filtros del 
LIMPIADOR_PRECIO_V2.py durante el proceso de scraping

Filtros aplicados:
- Precios mínimos por tipo de moto
- Eliminación de comerciales
- Eliminación de ubicaciones en islas
- Eliminación por kilometraje excesivo
- Eliminación de duplicados

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Tuple, Optional
from config import LIMPIEZA_CONFIG, get_modelo_config

class LimpiadorIntegrado:
    """Limpiador integrado para datos de motos de Wallapop"""
    
    def __init__(self, modelo_key: str):
        """
        Inicializar limpiador para un modelo específico
        
        Args:
            modelo_key: Clave del modelo de moto (ej: 'cb125r')
        """
        self.logger = logging.getLogger(__name__)
        self.modelo_key = modelo_key
        self.modelo_config = get_modelo_config(modelo_key)
        self.limpieza_config = LIMPIEZA_CONFIG
        
        # Determinar precio mínimo específico para el modelo
        self.precio_minimo = self._determine_model_min_price()
        
        self.logger.info(f" Limpiador iniciado para {self.modelo_config['nombre']}")
        self.logger.info(f" Precio mínimo configurado: {self.precio_minimo}€")
    
    def clean_data(self, df_motos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Limpiar datos de motos aplicando todos los filtros
        
        Args:
            df_motos: DataFrame con datos sin limpiar
            
        Returns:
            Tuple[DataFrame limpio, Dict con estadísticas]
        """
        if df_motos.empty:
            self.logger.warning(" DataFrame vacío, no hay nada que limpiar")
            return df_motos, self._create_empty_stats()
        
        self.logger.info(f" Iniciando limpieza de {len(df_motos)} motos")
        
        # Estadísticas iniciales
        stats = {
            'inicial': len(df_motos),
            'eliminados_precio': 0,
            'eliminados_comercial': 0,
            'eliminados_isla': 0,
            'eliminados_km': 0,
            'eliminados_duplicados': 0,
            'total_eliminados': 0,
            'final': 0,
            'precio_minimo': self.precio_minimo,
            'km_maximo': self.modelo_config.get('km_max', self.limpieza_config['filtros']['km_maximo_global'])
        }
        
        try:
            # Crear copia para no modificar el original
            df = df_motos.copy()
            
            # 1. Eliminar duplicados por URL si existe la columna
            if 'URL' in df.columns and self.limpieza_config['filtros']['eliminar_duplicados']:
                antes_duplicados = len(df)
                df = df.drop_duplicates(subset=['URL'], keep='first')
                stats['eliminados_duplicados'] = antes_duplicados - len(df)
                self.logger.info(f" Eliminados {stats['eliminados_duplicados']} duplicados por URL")
            
            # 2. Filtrar por precio
            if 'Precio' in df.columns:
                antes_precio = len(df)
                df = df[df['Precio'].apply(lambda x: self._is_valid_price(x, self.precio_minimo))]
                stats['eliminados_precio'] = antes_precio - len(df)
                self.logger.info(f" Eliminados {stats['eliminados_precio']} por precio < {self.precio_minimo}€")
            
            # 3. Filtrar comerciales
            if 'Vendedor' in df.columns and self.limpieza_config['filtros']['eliminar_comerciales']:
                antes_comercial = len(df)
                df = df[~df['Vendedor'].apply(self._is_commercial)]
                stats['eliminados_comercial'] = antes_comercial - len(df)
                self.logger.info(f" Eliminados {stats['eliminados_comercial']} comerciales")
            
            # 4. Filtrar islas
            if 'Ubicación' in df.columns and self.limpieza_config['filtros']['eliminar_islas']:
                antes_isla = len(df)
                df = df[~df['Ubicación'].apply(self._is_island_location)]
                stats['eliminados_isla'] = antes_isla - len(df)
                self.logger.info(f" Eliminados {stats['eliminados_isla']} de islas")
            
            # 5. Filtrar por kilometraje
            if 'Kilometraje' in df.columns:
                km_max = stats['km_maximo']
                antes_km = len(df)
                df = df[df['Kilometraje'].apply(lambda x: self._is_valid_km(x, km_max))]
                stats['eliminados_km'] = antes_km - len(df)
                self.logger.info(f" Eliminados {stats['eliminados_km']} por KM > {km_max}")
            
            # Calcular estadísticas finales
            stats['final'] = len(df)
            stats['total_eliminados'] = stats['inicial'] - stats['final']
            
            self._log_cleaning_summary(stats)
            
            return df, stats
            
        except Exception as e:
            self.logger.error(f" Error durante limpieza: {e}")
            return df_motos, self._create_empty_stats()
    
    def _determine_model_min_price(self) -> int:
        """Determinar precio mínimo específico según el modelo"""
        modelo_lower = self.modelo_config['nombre'].lower()
        
        # Precios mínimos específicos por tipo de moto
        if any(keyword in modelo_lower for keyword in ['agility', 'pcx']):
            # Scooters 125: precio mínimo más bajo
            return 600
        elif any(keyword in modelo_lower for keyword in ['z900', 'mt-07', 'mt07']):
            # Motos grandes: precio mínimo más alto
            return 2500
        elif any(keyword in modelo_lower for keyword in ['125', 'cb125r']):
            # Motos 125: precio mínimo medio
            return 1000
        else:
            # Default basado en configuración del modelo
            return max(self.modelo_config.get('precio_min', 1000), 
                      self.limpieza_config['filtros']['precio_minimo_global'])
    
    def _is_valid_price(self, price_text: str, min_price: int) -> bool:
        """Validar si el precio es válido (mayor al mínimo)"""
        if pd.isna(price_text) or not price_text:
            return False
        
        try:
            price_value = self._extract_price_number(price_text)
            return price_value >= min_price
        except:
            return False
    
    def _extract_price_number(self, price_text: str) -> float:
        """Extraer número del precio (similar al LIMPIADOR_PRECIO_V2)"""
        if not price_text or pd.isna(price_text):
            return 0.0
        
        price_str = str(price_text).strip()
        clean = re.sub(r'[^0-9,.]', '', price_str)
        
        if not clean:
            return 0.0
        
        try:
            # Formato español con punto para miles y coma para decimales (12.500,90)
            if '.' in clean and ',' in clean:
                parts = clean.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    whole_part = parts[0].replace('.', '')
                    return float(whole_part + '.' + parts[1])
                else:
                    return float(clean.replace('.', '').replace(',', ''))
            
            # Solo punto - determinar si es decimal o separador de miles
            elif '.' in clean and clean.count('.') == 1:
                parts = clean.split('.')
                if len(parts) == 2:
                    if len(parts[1]) <= 2:
                        return float(clean)
                    else:
                        return float(clean.replace('.', ''))
            
            # Solo coma - decimal en formato español (19,90)
            elif ',' in clean and clean.count(',') == 1:
                parts = clean.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    return float(clean.replace(',', '.'))
            
            # Solo números - entero simple
            elif clean.isdigit():
                return float(clean)
            
            # Múltiples puntos (12.500.000) - separadores de miles
            elif '.' in clean:
                number_str = clean.replace('.', '')
                if number_str.isdigit():
                    return float(number_str)
            
            # Extraer primer número válido
            numbers = re.findall(r'\d+', clean)
            if numbers:
                return float(numbers[0])
            
        except:
            pass
        
        return 0.0
    
    def _is_commercial(self, vendedor_text: str) -> bool:
        """Detectar si el vendedor es comercial"""
        if pd.isna(vendedor_text) or not vendedor_text:
            return False
        
        vendedor_lower = str(vendedor_text).lower()
        
        # Verificar palabras clave comerciales
        commercial_keywords = self.limpieza_config['comerciales_keywords']
        
        for keyword in commercial_keywords:
            if keyword in vendedor_lower:
                return True
        
        # Detectar patrones adicionales
        commercial_patterns = [
            r'\bS\.?L\.?\b',  # S.L.
            r'\bS\.?A\.?\b',  # S.A.
            r'\bLtd\.?\b',    # Ltd
            r'\b\d{2,3}[\-\s]*\d{3}[\-\s]*\d{3}\b',  # Teléfonos empresariales
            r'@',  # Emails (generalmente comerciales)
        ]
        
        for pattern in commercial_patterns:
            if re.search(pattern, vendedor_lower):
                return True
        
        return False
    
    def _is_island_location(self, ubicacion_text: str) -> bool:
        """Detectar si la ubicación está en una isla"""
        if pd.isna(ubicacion_text) or not ubicacion_text:
            return False
        
        ubicacion_lower = str(ubicacion_text).lower()
        
        # Verificar islas configuradas
        islas = self.limpieza_config['islas_spain']
        
        for isla in islas:
            if isla in ubicacion_lower:
                return True
        
        return False
    
    def _is_valid_km(self, km_text: str, max_km: int) -> bool:
        """Validar si el kilometraje es válido (menor al máximo)"""
        if pd.isna(km_text) or not km_text:
            return True  # Aceptar si no hay datos de KM
        
        try:
            km_value = self._extract_km_number(km_text)
            return km_value <= max_km
        except:
            return True
    
    def _extract_km_number(self, km_text: str) -> float:
        """Extraer número del kilometraje"""
        if not km_text or pd.isna(km_text):
            return 0.0
        
        km_str = str(km_text).strip()
        clean = re.sub(r'[^0-9,.]', '', km_str)
        
        if not clean:
            return 0.0
        
        try:
            # Similar a precio pero más simple
            if ',' in clean and clean.count(',') == 1:
                parts = clean.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    return float(clean.replace(',', '.'))
            
            if '.' in clean and clean.count('.') == 1:
                parts = clean.split('.')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    return float(clean)
                else:
                    return float(clean.replace('.', ''))
            
            if clean.isdigit():
                return float(clean)
            
            # Extraer primer número
            numbers = re.findall(r'\d+', clean)
            if numbers:
                return float(numbers[0])
            
        except:
            pass
        
        return 0.0
    
    def _create_empty_stats(self) -> Dict:
        """Crear estadísticas vacías"""
        return {
            'inicial': 0,
            'eliminados_precio': 0,
            'eliminados_comercial': 0,
            'eliminados_isla': 0,
            'eliminados_km': 0,
            'eliminados_duplicados': 0,
            'total_eliminados': 0,
            'final': 0,
            'precio_minimo': self.precio_minimo,
            'km_maximo': 0
        }
    
    def _log_cleaning_summary(self, stats: Dict):
        """Registrar resumen de limpieza"""
        self.logger.info(f" Resumen de limpieza completado:")
        self.logger.info(f"    Inicial: {stats['inicial']} motos")
        self.logger.info(f"    Eliminados por precio: {stats['eliminados_precio']}")
        self.logger.info(f"    Eliminados comerciales: {stats['eliminados_comercial']}")
        self.logger.info(f"    Eliminados de islas: {stats['eliminados_isla']}")
        self.logger.info(f"    Eliminados por KM: {stats['eliminados_km']}")
        self.logger.info(f"    Duplicados eliminados: {stats['eliminados_duplicados']}")
        self.logger.info(f"    Total eliminados: {stats['total_eliminados']}")
        self.logger.info(f"    Final: {stats['final']} motos")
        
        if stats['inicial'] > 0:
            porcentaje_conservado = (stats['final'] / stats['inicial']) * 100
            self.logger.info(f"    Conservado: {porcentaje_conservado:.1f}%")

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def clean_model_data(modelo_key: str, df_motos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Función helper para limpiar datos de un modelo
    
    Args:
        modelo_key: Clave del modelo
        df_motos: DataFrame con datos sin limpiar
        
    Returns:
        Tuple[DataFrame limpio, Dict con estadísticas]
    """
    limpiador = LimpiadorIntegrado(modelo_key)
    return limpiador.clean_data(df_motos)

def test_limpiador_integrado():
    """Función de prueba para el limpiador integrado"""
    print(" Probando limpiador integrado...")
    
    # Crear datos de prueba con problemas típicos
    test_data = {
        'Título': [
            'Honda CB125R 2020',
            'Honda CB125R Comercial',
            'Honda CB125R Barata',
            'Honda CB125R Canarias',
            'Honda CB125R Muchos KM'
        ],
        'Precio': ['3.500€', '4.200€', '400€', '3.800€', '3.200€'],
        'Kilometraje': ['5.000', '8.000', '3.000', '7.000', '45.000'],
        'Año': ['2020', '2021', '2019', '2020', '2018'],
        'Vendedor': ['Particular', 'Concesionario BMW', 'Particular', 'Particular', 'Particular'],
        'Ubicación': ['Madrid', 'Barcelona', 'Valencia', 'Tenerife', 'Sevilla'],
        'URL': ['url1', 'url2', 'url3', 'url4', 'url5']
    }
    
    df_test = pd.DataFrame(test_data)
    
    try:
        limpiador = LimpiadorIntegrado('cb125r')
        df_limpio, stats = limpiador.clean_data(df_test)
        
        print(" Limpiador funcionando correctamente")
        print(f" De {stats['inicial']} motos -> {stats['final']} conservadas")
        print(f" Eliminadas: {stats['total_eliminados']}")
        
        if not df_limpio.empty:
            print("\n Motos que pasaron el filtro:")
            for i, (_, moto) in enumerate(df_limpio.iterrows()):
                print(f"   {i+1}. {moto['Título']} - {moto['Precio']} - {moto['Ubicación']}")
        
        return True
        
    except Exception as e:
        print(f" Error en limpiador: {e}")
        return False

if __name__ == "__main__":
    # Prueba del limpiador
    test_limpiador_integrado()