"""
================================================================================
                    CALCULADOR DE RENTABILIDAD - WALLAPOP MOTOS SCRAPER                    
================================================================================

Sistema de cálculo de rentabilidad para motos basado en precio, kilometraje y año
Ordena las motos de más a menos rentables según fórmula ponderada

Fórmula de Rentabilidad:
- Precio (40%): Menor precio = mayor puntuación
- Kilometraje (35%): Menos km = mayor puntuación  
- Año (25%): Más reciente = mayor puntuación

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
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
    """Calculador de rentabilidad para motos de Wallapop"""
    
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
        self.normalizacion = RENTABILIDAD_CONFIG["normalizacion"]
        
        self.logger.info(f" Calculador iniciado para {self.modelo_config['nombre']}")
    
    def calculate_rentabilidad(self, df_motos: pd.DataFrame) -> pd.DataFrame:
        """
        Calcular rentabilidad para todas las motos y ordenar por score
        
        Args:
            df_motos: DataFrame con los datos de las motos
            
        Returns:
            DataFrame ordenado por rentabilidad (mayor a menor)
        """
        if df_motos.empty:
            self.logger.warning(" DataFrame vacío, no se puede calcular rentabilidad")
            return df_motos
        
        self.logger.info(f" Calculando rentabilidad para {len(df_motos)} motos")
        
        try:
            # Crear copia para no modificar el original
            df = df_motos.copy()
            
            # Extraer valores numéricos
            df['Precio_Numerico'] = df['Precio'].apply(self._extract_price)
            df['KM_Numerico'] = df['Kilometraje'].apply(self._extract_km)
            df['Año_Numerico'] = df['Año'].apply(self._extract_year)
            
            # Filtrar filas con datos válidos
            df_valido = df[
                (df['Precio_Numerico'] > 0) & 
                (df['KM_Numerico'] >= 0) & 
                (df['Año_Numerico'] > 0)
            ].copy()
            
            if df_valido.empty:
                self.logger.warning(" No hay motos con datos válidos para calcular rentabilidad")
                return df_motos
            
            # Calcular scores individuales
            df_valido['Score_Precio'] = self._calculate_price_score(df_valido['Precio_Numerico'])
            df_valido['Score_KM'] = self._calculate_km_score(df_valido['KM_Numerico'])
            df_valido['Score_Año'] = self._calculate_year_score(df_valido['Año_Numerico'])
            
            # Calcular score total de rentabilidad
            df_valido['Rentabilidad_Score'] = (
                df_valido['Score_Precio'] * self.pesos['precio'] +
                df_valido['Score_KM'] * self.pesos['kilometraje'] +
                df_valido['Score_Año'] * self.pesos['año']
            )
            
            # Añadir categoría de rentabilidad
            df_valido['Categoria_Rentabilidad'] = df_valido['Rentabilidad_Score'].apply(self._get_rentabilidad_category)
            
            # Ordenar por rentabilidad (mayor a menor)
            df_ordenado = df_valido.sort_values('Rentabilidad_Score', ascending=False)
            
            # Añadir ranking
            df_ordenado['Ranking_Rentabilidad'] = range(1, len(df_ordenado) + 1)
            
            # Añadir motos sin datos válidos al final
            df_sin_datos = df[~df.index.isin(df_valido.index)].copy()
            if not df_sin_datos.empty:
                df_sin_datos['Rentabilidad_Score'] = 0
                df_sin_datos['Categoria_Rentabilidad'] = "Sin datos"
                df_sin_datos['Ranking_Rentabilidad'] = len(df_ordenado) + 1
                df_ordenado = pd.concat([df_ordenado, df_sin_datos], ignore_index=True)
            
            # Limpiar columnas auxiliares si no se quieren mostrar
            columnas_a_eliminar = ['Precio_Numerico', 'KM_Numerico', 'Año_Numerico', 
                                 'Score_Precio', 'Score_KM', 'Score_Año']
            df_final = df_ordenado.drop(columns=columnas_a_eliminar, errors='ignore')
            
            # Reordenar columnas para poner rentabilidad al principio
            columnas = list(df_final.columns)
            if 'Rentabilidad_Score' in columnas:
                columnas.remove('Rentabilidad_Score')
                columnas.insert(0, 'Rentabilidad_Score')
            if 'Categoria_Rentabilidad' in columnas:
                columnas.remove('Categoria_Rentabilidad')
                columnas.insert(1, 'Categoria_Rentabilidad')
            if 'Ranking_Rentabilidad' in columnas:
                columnas.remove('Ranking_Rentabilidad')
                columnas.insert(2, 'Ranking_Rentabilidad')
            
            df_final = df_final[columnas]
            
            self._log_rentabilidad_stats(df_final)
            
            return df_final
            
        except Exception as e:
            self.logger.error(f" Error calculando rentabilidad: {e}")
            return df_motos
    
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
            return pd.Series([self.normalizacion['precio_max_score']] * len(precios))
        
        # Normalizar: menor precio = mayor score
        precio_min = precios.min()
        precio_max = precios.max()
        
        # Score inverso: precio más bajo = score más alto
        scores = self.normalizacion['precio_max_score'] * (precio_max - precios) / (precio_max - precio_min)
        
        return scores.clip(0, self.normalizacion['precio_max_score'])
    
    def _calculate_km_score(self, kms: pd.Series) -> pd.Series:
        """Calcular score de kilometraje (menos km = mayor score)"""
        if kms.empty or kms.max() == kms.min():
            return pd.Series([self.normalizacion['km_max_score']] * len(kms))
        
        # Normalizar: menos km = mayor score
        km_min = kms.min()
        km_max = kms.max()
        
        # Score inverso: menos km = score más alto
        scores = self.normalizacion['km_max_score'] * (km_max - kms) / (km_max - km_min)
        
        return scores.clip(0, self.normalizacion['km_max_score'])
    
    def _calculate_year_score(self, años: pd.Series) -> pd.Series:
        """Calcular score de año (más reciente = mayor score)"""
        if años.empty or años.max() == años.min():
            return pd.Series([self.normalizacion['año_max_score']] * len(años))
        
        # Normalizar: año más reciente = mayor score
        año_min = años.min()
        año_max = años.max()
        
        # Score directo: año más reciente = score más alto
        scores = self.normalizacion['año_max_score'] * (años - año_min) / (año_max - año_min)
        
        return scores.clip(0, self.normalizacion['año_max_score'])
    
    def _get_rentabilidad_category(self, score: float) -> str:
        """Obtener categoría de rentabilidad según el score"""
        max_score = sum(self.normalizacion.values())
        porcentaje = (score / max_score) * 100
        
        if porcentaje >= 80:
            return " Excelente"
        elif porcentaje >= 65:
            return " Muy Buena"
        elif porcentaje >= 50:
            return " Buena"
        elif porcentaje >= 35:
            return " Regular"
        elif porcentaje >= 20:
            return " Baja"
        else:
            return " Muy Baja"
    
    def _log_rentabilidad_stats(self, df_rentabilidad: pd.DataFrame):
        """Registrar estadísticas de rentabilidad"""
        if df_rentabilidad.empty:
            return
        
        total_motos = len(df_rentabilidad)
        motos_con_score = len(df_rentabilidad[df_rentabilidad['Rentabilidad_Score'] > 0])
        
        if motos_con_score > 0:
            score_medio = df_rentabilidad[df_rentabilidad['Rentabilidad_Score'] > 0]['Rentabilidad_Score'].mean()
            score_max = df_rentabilidad['Rentabilidad_Score'].max()
            
            # Contar por categorías
            categorias = df_rentabilidad['Categoria_Rentabilidad'].value_counts()
            
            self.logger.info(f" Estadísticas de rentabilidad:")
            self.logger.info(f"   • Total motos: {total_motos}")
            self.logger.info(f"   • Con score válido: {motos_con_score}")
            self.logger.info(f"   • Score medio: {score_medio:.2f}")
            self.logger.info(f"   • Score máximo: {score_max:.2f}")
            
            for categoria, count in categorias.head(3).items():
                self.logger.info(f"   • {categoria}: {count} motos")

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def calculate_model_rentabilidad(modelo_key: str, df_motos: pd.DataFrame) -> pd.DataFrame:
    """
    Función helper para calcular rentabilidad de un modelo
    
    Args:
        modelo_key: Clave del modelo
        df_motos: DataFrame con datos de motos
        
    Returns:
        DataFrame ordenado por rentabilidad
    """
    calculator = RentabilidadCalculator(modelo_key)
    return calculator.calculate_rentabilidad(df_motos)

def test_rentabilidad_calculator():
    """Función de prueba para el calculador de rentabilidad"""
    print(" Probando calculador de rentabilidad...")
    
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
        
        print(" Calculador funcionando correctamente")
        print(f" {len(df_resultado)} motos procesadas")
        
        if not df_resultado.empty:
            print("\n Top 3 más rentables:")
            for i, (_, moto) in enumerate(df_resultado.head(3).iterrows()):
                print(f"   {i+1}. {moto['Título']} - Score: {moto.get('Rentabilidad_Score', 0):.2f}")
        
        return True
        
    except Exception as e:
        print(f" Error en calculador: {e}")
        return False

if __name__ == "__main__":
    # Prueba del calculador
    test_rentabilidad_calculator()