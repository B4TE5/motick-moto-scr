"""
================================================================================
                        CALCULADOR DE RENTABILIDAD                    
================================================================================

Autor: Carlos Peraza
Versión: 1.7
Fecha: Agosto 2025

================================================================================
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from config import RENTABILIDAD_CONFIG, get_modelo_config

class RentabilidadCalculator:
    """Calculador de rentabilidad simplificado para motos de Wallapop"""
    
    def __init__(self, modelo_key: str):
        """
        Inicializar calculador para un modelo específico
        
        Args:
            modelo_key: Clave del modelo de moto (ej: 'cb125r')
        """
        self.logger = logging.getLogger(__name__)
        self.modelo_key = modelo_key
        self.modelo_config = get_modelo_config(modelo_key)
        self.pesos = RENTABILIDAD_CONFIG["pesos"]
        
        self.logger.info(f"Calculador simplificado iniciado para {self.modelo_config['nombre']}")
    
    def calculate_rentabilidad(self, df_motos: pd.DataFrame) -> pd.DataFrame:
        """
        Calcular rentabilidad simplificada y ordenar por mejor rentabilidad
        SOLO genera categoría simple, NO scores numéricos
        
        Args:
            df_motos: DataFrame con los datos de las motos
            
        Returns:
            DataFrame ordenado por rentabilidad con categoría simple
        """
        if df_motos.empty:
            self.logger.warning("DataFrame vacío, no se puede calcular rentabilidad")
            return df_motos
        
        self.logger.info(f"Calculando rentabilidad simplificada para {len(df_motos)} motos")
        
        try:
            # Crear copia para no modificar el original
            df = df_motos.copy()
            
            # Extraer valores numéricos para análisis interno
            df['_precio_num'] = df['Precio'].apply(self._extract_price)
            df['_km_num'] = df['Kilometraje'].apply(self._extract_km)
            df['_año_num'] = df['Año'].apply(self._extract_year)
            
            # Filtrar filas con datos válidos para análisis
            df_valido = df[
                (df['_precio_num'] > 0) & 
                (df['_km_num'] >= 0) & 
                (df['_año_num'] > 0)
            ].copy()
            
            if df_valido.empty:
                self.logger.warning("No hay motos con datos válidos para calcular rentabilidad")
                # Asignar "Sin datos" a todas y retornar
                df['Rentabilidad'] = "Sin datos"
                return self._reorder_columns(df)
            
            # Calcular scores internos (NO se mostrarán)
            df_valido['_score_precio'] = self._calculate_price_score(df_valido['_precio_num'])
            df_valido['_score_km'] = self._calculate_km_score(df_valido['_km_num'])
            df_valido['_score_año'] = self._calculate_year_score(df_valido['_año_num'])
            
            # Calcular score total interno para ordenamiento
            df_valido['_score_total'] = (
                df_valido['_score_precio'] * self.pesos['precio'] +
                df_valido['_score_km'] * self.pesos['kilometraje'] +
                df_valido['_score_año'] * self.pesos['año']
            )
            
            # Generar SOLO categoría de rentabilidad (lo que se mostrará)
            df_valido['Rentabilidad'] = df_valido['_score_total'].apply(self._get_categoria_simple)
            
            # Ordenar por score interno (mayor a menor rentabilidad)
            df_ordenado = df_valido.sort_values('_score_total', ascending=False)
            
            # Añadir motos sin datos válidos al final
            df_sin_datos = df[~df.index.isin(df_valido.index)].copy()
            if not df_sin_datos.empty:
                df_sin_datos['Rentabilidad'] = "Sin datos"
                df_ordenado = pd.concat([df_ordenado, df_sin_datos], ignore_index=True)
            
            # ELIMINAR todas las columnas auxiliares (scores, etc.)
            columnas_a_eliminar = [
                '_precio_num', '_km_num', '_año_num', 
                '_score_precio', '_score_km', '_score_año', '_score_total'
            ]
            df_final = df_ordenado.drop(columns=columnas_a_eliminar, errors='ignore')
            
            # Reordenar columnas según el orden especificado
            df_final = self._reorder_columns(df_final)
            
            self._log_rentabilidad_stats(df_final)
            
            return df_final
            
        except Exception as e:
            self.logger.error(f"Error calculando rentabilidad: {e}")
            return df_motos
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reordenar columnas según el orden especificado:
        Título, Precio, Kilometraje, Año, Rentabilidad, Vendedor, Ubicación, Fecha_Publicacion, URL, Fecha_Extraccion
        """
        orden_deseado = [
            'Título', 'Precio', 'Kilometraje', 'Año', 'Rentabilidad', 
            'Vendedor', 'Ubicación', 'Fecha_Publicacion', 'URL', 'Fecha_Extraccion'
        ]
        
        # Obtener columnas existentes en el orden deseado
        columnas_ordenadas = []
        for col in orden_deseado:
            if col in df.columns:
                columnas_ordenadas.append(col)
        
        # Añadir cualquier columna restante que no esté en el orden
        for col in df.columns:
            if col not in columnas_ordenadas:
                columnas_ordenadas.append(col)
        
        return df[columnas_ordenadas]
    
    def _extract_price(self, price_text: str) -> float:
        """Extraer valor numérico del precio"""
        if pd.isna(price_text) or not price_text:
            return 0.0
        
        try:
            price_str = str(price_text).strip()
            # Quitar caracteres no numéricos excepto puntos y comas
            clean = re.sub(r'[^0-9,.]', '', price_str)
            
            if not clean:
                return 0.0
            
            # Manejar formato español (12.500,90) vs inglés (12,500.90)
            if ',' in clean and '.' in clean:
                # Si hay ambos, asumir formato español
                parts = clean.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    whole_part = parts[0].replace('.', '')
                    return float(whole_part + '.' + parts[1])
                else:
                    return float(clean.replace('.', '').replace(',', ''))
            elif ',' in clean:
                # Solo coma - puede ser decimal español
                if clean.count(',') == 1 and len(clean.split(',')[1]) <= 2:
                    return float(clean.replace(',', '.'))
                else:
                    return float(clean.replace(',', ''))
            elif '.' in clean:
                # Solo punto - puede ser separador de miles o decimal
                if clean.count('.') == 1 and len(clean.split('.')[1]) <= 2:
                    return float(clean)
                else:
                    return float(clean.replace('.', ''))
            else:
                return float(clean)
                
        except (ValueError, AttributeError):
            return 0.0
    
    def _extract_km(self, km_text: str) -> float:
        """Extraer valor numérico del kilometraje"""
        if pd.isna(km_text) or not km_text:
            return 0.0
        
        try:
            km_str = str(km_text).strip().lower()
            
            # Extraer números
            numbers = re.findall(r'\d+', km_str.replace('.', '').replace(',', ''))
            if numbers:
                km_value = float(numbers[0])
                
                # Convertir a km si está en metros (muy poco común pero posible)
                if 'metro' in km_str and km_value > 10000:
                    km_value = km_value / 1000
                
                return km_value
            
            return 0.0
            
        except (ValueError, AttributeError):
            return 0.0
    
    def _extract_year(self, year_text: str) -> int:
        """Extraer año numérico"""
        if pd.isna(year_text) or not year_text:
            return 0
        
        try:
            year_str = str(year_text).strip()
            
            # Buscar año de 4 dígitos
            year_match = re.search(r'(19|20)\d{2}', year_str)
            if year_match:
                year = int(year_match.group())
                # Validar que sea un año razonable
                current_year = datetime.now().year
                if 1990 <= year <= current_year + 1:
                    return year
            
            return 0
            
        except (ValueError, AttributeError):
            return 0
    
    def _calculate_price_score(self, precios: pd.Series) -> pd.Series:
        """Calcular score de precio (menor precio = mayor score)"""
        if precios.empty or precios.max() == precios.min():
            return pd.Series([5.0] * len(precios))  # Score neutral
        
        # Normalizar: menor precio = mayor score (0-10)
        precio_min = precios.min()
        precio_max = precios.max()
        
        scores = 10.0 * (precio_max - precios) / (precio_max - precio_min)
        
        return scores.clip(0, 10)
    
    def _calculate_km_score(self, kms: pd.Series) -> pd.Series:
        """Calcular score de kilometraje (menos km = mayor score)"""
        if kms.empty or kms.max() == kms.min():
            return pd.Series([5.0] * len(kms))  # Score neutral
        
        # Normalizar: menos km = mayor score (0-10)
        km_min = kms.min()
        km_max = kms.max()
        
        scores = 10.0 * (km_max - kms) / (km_max - km_min)
        
        return scores.clip(0, 10)
    
    def _calculate_year_score(self, años: pd.Series) -> pd.Series:
        """Calcular score de año (más reciente = mayor score)"""
        if años.empty or años.max() == años.min():
            return pd.Series([5.0] * len(años))  # Score neutral
        
        # Normalizar: año más reciente = mayor score (0-10)
        año_min = años.min()
        año_max = años.max()
        
        scores = 10.0 * (años - año_min) / (año_max - año_min)
        
        return scores.clip(0, 10)
    
    def _get_categoria_simple(self, score: float) -> str:
        """
        Convertir score interno a categoría simple de rentabilidad
        SOLO se mostrarán estas categorías, NO el score numérico
        """
        # Score total máximo = 10 (promedio ponderado de los 3 scores)
        porcentaje = (score / 10.0) * 100
        
        if porcentaje >= 85:
            return "Excelente"
        elif porcentaje >= 70:
            return "Muy Buena"
        elif porcentaje >= 55:
            return "Buena"
        elif porcentaje >= 40:
            return "Regular"
        elif porcentaje >= 25:
            return "Baja"
        else:
            return "Muy Baja"
    
    def _log_rentabilidad_stats(self, df_rentabilidad: pd.DataFrame):
        """Registrar estadísticas de rentabilidad simplificadas"""
        if df_rentabilidad.empty:
            return
        
        total_motos = len(df_rentabilidad)
        
        # Contar por categorías
        if 'Rentabilidad' in df_rentabilidad.columns:
            categorias = df_rentabilidad['Rentabilidad'].value_counts()
            
            self.logger.info(f"Estadísticas de rentabilidad:")
            self.logger.info(f"   • Total motos: {total_motos}")
            
            for categoria, count in categorias.items():
                porcentaje = (count / total_motos) * 100
                self.logger.info(f"   • {categoria}: {count} motos ({porcentaje:.1f}%)")

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def calculate_model_rentabilidad(modelo_key: str, df_motos: pd.DataFrame) -> pd.DataFrame:
    """
    Función helper para calcular rentabilidad simplificada de un modelo
    
    Args:
        modelo_key: Clave del modelo
        df_motos: DataFrame con datos de motos
        
    Returns:
        DataFrame ordenado por rentabilidad con categorías simples
    """
    calculator = RentabilidadCalculator(modelo_key)
    return calculator.calculate_rentabilidad(df_motos)

def test_rentabilidad_calculator_simplificado():
    """Función de prueba para el calculador de rentabilidad simplificado"""
    print("Probando calculador de rentabilidad simplificado...")
    
    # Crear datos de prueba
    test_data = {
        'Título': ['Honda CB125R 2020', 'Honda CB125R 2019', 'Honda CB125R 2021'],
        'Precio': ['3.500€', '2.800€', '4.200€'],
        'Kilometraje': ['5.000', '12.000', '2.500'],
        'Año': ['2020', '2019', '2021'],
        'Vendedor': ['Particular', 'Particular', 'Particular'],
        'Ubicación': ['Madrid', 'Barcelona', 'Valencia']
    }
    
    df_test = pd.DataFrame(test_data)
    
    try:
        calculator = RentabilidadCalculator('cb125r')
        df_resultado = calculator.calculate_rentabilidad(df_test)
        
        print("Calculador simplificado funcionando correctamente")
        print(f"{len(df_resultado)} motos procesadas")
        
        if not df_resultado.empty:
            print("\nTop 3 más rentables:")
            for i, (_, moto) in enumerate(df_resultado.head(3).iterrows()):
                print(f"   {i+1}. {moto['Título']} - Rentabilidad: {moto.get('Rentabilidad', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"Error en calculador: {e}")
        return False

if __name__ == "__main__":
    # Prueba del calculador simplificado
    test_rentabilidad_calculator_simplificado()
