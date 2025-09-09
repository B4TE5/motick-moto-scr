"""
================================================================================
                            GOOGLE SHEETS MANAGER                   
================================================================================

Autor: Carlos Peraza
Versión: 2.0
Fecha: Agosto 2025

================================================================================
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import os
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
from config import GOOGLE_SHEETS_CONFIG, get_sheet_id, is_github_actions, get_modelo_config

class GoogleSheetsManager:
    """Gestor corregido para operaciones con Google Sheets"""
    
    def __init__(self, credentials_source: str = None, sheet_id: str = None):
        """Inicializar el gestor de Google Sheets"""
        self.logger = logging.getLogger(__name__)
        self.sheet_id = sheet_id or get_sheet_id()
        
        if not self.sheet_id:
            raise ValueError("Sheet ID no proporcionado y no encontrado en variables de entorno")
        
        # Configurar credenciales
        self.credentials = self._setup_credentials(credentials_source)
        self.client = gspread.authorize(self.credentials)
        
        # Conectar al spreadsheet
        try:
            self.spreadsheet = self.client.open_by_key(self.sheet_id)
            self.logger.info(f"Conectado a Google Sheet: {self.spreadsheet.title}")
        except Exception as e:
            self.logger.error(f"Error conectando a Google Sheet: {e}")
            raise
    
    def _setup_credentials(self, credentials_source: str = None) -> Credentials:
        """Configurar credenciales de Google según el entorno"""
        scopes = GOOGLE_SHEETS_CONFIG["scopes"]
        
        # Detectar fuente de credenciales automáticamente si no se especifica
        if credentials_source is None:
            if is_github_actions():
                credentials_source = "ENV"
            elif os.path.exists(GOOGLE_SHEETS_CONFIG["local_credentials_file"]):
                credentials_source = GOOGLE_SHEETS_CONFIG["local_credentials_file"]
            else:
                raise ValueError("No se encontraron credenciales válidas")
        
        try:
            if credentials_source == "ENV":
                # GitHub Actions - desde variable de entorno
                credentials_json = os.getenv(GOOGLE_SHEETS_CONFIG["credentials_env_var"])
                if not credentials_json:
                    raise ValueError("Variable de entorno de credenciales no encontrada")
                
                credentials_dict = json.loads(credentials_json)
                return Credentials.from_service_account_info(credentials_dict, scopes=scopes)
            
            elif os.path.isfile(credentials_source):
                # Archivo local
                return Credentials.from_service_account_file(credentials_source, scopes=scopes)
            
            else:
                # Asumir que es un JSON string directo
                credentials_dict = json.loads(credentials_source)
                return Credentials.from_service_account_info(credentials_dict, scopes=scopes)
                
        except Exception as e:
            self.logger.error(f"Error configurando credenciales: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Probar la conexión a Google Sheets"""
        try:
            title = self.spreadsheet.title
            worksheets = len(self.spreadsheet.worksheets())
            self.logger.info(f"Conexión exitosa a '{title}' ({worksheets} hojas)")
            return True
        except Exception as e:
            self.logger.error(f"Error probando conexión: {e}")
            return False
    
    def upload_modelo_data(self, 
                          modelo_key: str, 
                          df_motos: pd.DataFrame, 
                          sheet_name: str = None,
                          overwrite: bool = True) -> bool:
        """
        CORREGIDO: Subir datos con formato correcto y nombre de hoja con fecha
        """
        try:
            modelo_config = get_modelo_config(modelo_key)
            
            # NOMBRE DE HOJA CORREGIDO: CBR125R 08/09/25
            if not sheet_name:
                modelo_simple = self._get_simple_model_name(modelo_key)
                fecha_actual = datetime.now().strftime("%d/%m/%y")
                sheet_name = f"{modelo_simple} {fecha_actual}"
            
            self.logger.info(f"Subiendo {len(df_motos)} motos del modelo {modelo_key} a hoja '{sheet_name}'")
            
            # PREPARAR DATOS CON FORMATO CORRECTO
            df_formatted = self._format_dataframe_for_sheets(df_motos)
            
            # Obtener o crear la hoja
            worksheet = self._get_or_create_worksheet(sheet_name, len(df_formatted) + 10, len(df_formatted.columns) + 2)
            
            if overwrite:
                worksheet.clear()
            
            # Preparar datos con encabezados
            headers = df_formatted.columns.tolist()
            data_rows = df_formatted.values.tolist()
            
            # Convertir valores NaN a string vacío para Google Sheets
            clean_data_rows = []
            for row in data_rows:
                clean_row = []
                for cell in row:
                    if pd.isna(cell):
                        clean_row.append("")
                    else:
                        clean_row.append(str(cell))
                clean_data_rows.append(clean_row)
            
            all_data = [headers] + clean_data_rows
            
            # Subir datos
            worksheet.update(all_data)
            
            # Añadir metadatos
            self._add_metadata_to_sheet(worksheet, modelo_key, df_formatted, len(all_data))
            
            self.logger.info(f"{len(df_formatted)} motos subidas exitosamente a '{sheet_name}'")
            self.logger.info(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error subiendo modelo {modelo_key}: {e}")
            return False
    
    def _get_simple_model_name(self, modelo_key: str) -> str:
        """Obtener nombre simple del modelo para la hoja"""
        model_names = {
            'cb125r': 'CB125R',      # CORREGIDO: CB125R no CBR125R
            'pcx125': 'PCX125', 
            'agility125': 'AGILITY125',
            'z900': 'Z900',
            'mt07': 'MT07'
        }
        
        return model_names.get(modelo_key, modelo_key.upper())
    
    def _format_dataframe_for_sheets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        CORREGIDO: Formatear DataFrame con orden correcto y sin columnas no deseadas
        
        Orden deseado: Título, Precio, Kilometraje, Año, Rentabilidad, Vendedor, 
                      Ubicación, Fecha_Publicacion, URL, Fecha_Extraccion
        """
        # COLUMNAS NO DESEADAS QUE SE DEBEN ELIMINAR
        columns_to_remove = [
            'Rentabilidad_Score',
            'Ranking_Rentabilidad', 
            'Descripcion',
            'Categoria_Rentabilidad'
        ]
        
        # Crear copia para no modificar el original
        df_clean = df.copy()
        
        # Eliminar columnas no deseadas
        for col in columns_to_remove:
            if col in df_clean.columns:
                df_clean = df_clean.drop(columns=[col])
        
        # AÑADIR COLUMNA RENTABILIDAD SIMPLIFICADA
        if 'Rentabilidad_Score' in df.columns:
            # Convertir score a categoría simple
            df_clean['Rentabilidad'] = df['Rentabilidad_Score'].apply(self._score_to_simple_category)
        else:
            df_clean['Rentabilidad'] = 'No calculada'
        
        # ORDEN CORRECTO DE COLUMNAS
        desired_columns = [
            'Título',
            'Precio', 
            'Kilometraje',
            'Año',
            'Rentabilidad',
            'Vendedor',
            'Ubicación',
            'Fecha_Publicacion',
            'URL',
            'Fecha_Extraccion'
        ]
        
        # Reordenar columnas según el orden deseado
        final_columns = []
        for col in desired_columns:
            if col in df_clean.columns:
                final_columns.append(col)
        
        # Añadir cualquier columna restante que no esté en la lista
        for col in df_clean.columns:
            if col not in final_columns:
                final_columns.append(col)
        
        return df_clean[final_columns]
    
    def _score_to_simple_category(self, score) -> str:
        """Convertir score numérico a categoría simple"""
        if pd.isna(score):
            return "No calculada"
        
        try:
            score_value = float(score)
            if score_value >= 8:
                return "Excelente"
            elif score_value >= 6:
                return "Buena"
            elif score_value >= 4:
                return "Regular"
            else:
                return "Baja"
        except:
            return "No calculada"
    
    def _get_or_create_worksheet(self, sheet_name: str, rows: int, cols: int):
        """Obtener hoja existente o crear una nueva"""
        try:
            # Intentar obtener hoja existente
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self.logger.info(f"Usando hoja existente: {sheet_name}")
            return worksheet
        except gspread.WorksheetNotFound:
            # Crear nueva hoja
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name,
                rows=rows,
                cols=cols
            )
            self.logger.info(f"Nueva hoja creada: {sheet_name}")
            return worksheet
    
    def _add_metadata_to_sheet(self, worksheet, modelo_key: str, df_motos: pd.DataFrame, data_end_row: int):
        """Añadir metadatos al final de la hoja"""
        try:
            # Calcular estadísticas
            total_motos = len(df_motos)
            precio_medio = self._calculate_average_price(df_motos)
            km_medio = self._calculate_average_km(df_motos)
            
            # Crear metadatos
            metadata_start = data_end_row + 3
            metadata = [
                ["=== METADATOS ===", "", "", ""],
                ["Modelo", modelo_key.upper(), "", ""],
                ["Última actualización", datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "", ""],
                ["Total motos encontradas", total_motos, "", ""],
                ["Precio medio", f"{precio_medio:.0f}€" if precio_medio > 0 else "N/A", "", ""],
                ["KM medio", f"{km_medio:.0f}" if km_medio > 0 else "N/A", "", ""],
                ["Origen", "Wallapop Scraper Automation", "", ""],
                ["Ordenamiento", "Por rentabilidad (mayor a menor)", "", ""],
                ["Estado", "Completado", "", ""]
            ]
            
            # Subir metadatos
            start_cell = f"A{metadata_start}"
            worksheet.update(start_cell, metadata)
            
        except Exception as e:
            self.logger.warning(f"Error añadiendo metadatos: {e}")
    
    def _calculate_average_price(self, df: pd.DataFrame) -> float:
        """Calcular precio medio"""
        if 'Precio' not in df.columns:
            return 0.0
        
        prices = []
        for price_text in df['Precio']:
            try:
                if pd.isna(price_text) or price_text == "No especificado":
                    continue
                
                # Extraer número del precio
                numbers = re.findall(r'\d+', str(price_text).replace('.', '').replace(',', ''))
                if numbers:
                    price_value = float(numbers[0])
                    if 500 <= price_value <= 60000:  # Rango válido
                        prices.append(price_value)
            except:
                continue
        
        return sum(prices) / len(prices) if prices else 0.0
    
    def _calculate_average_km(self, df: pd.DataFrame) -> float:
        """Calcular kilometraje medio"""
        if 'Kilometraje' not in df.columns:
            return 0.0
        
        kms = []
        for km_text in df['Kilometraje']:
            try:
                if pd.isna(km_text) or km_text == "No especificado":
                    continue
                
                # Extraer número del kilometraje
                numbers = re.findall(r'\d+', str(km_text).replace('.', '').replace(',', ''))
                if numbers:
                    km_value = float(numbers[0])
                    if 0 <= km_value <= 200000:  # Rango válido
                        kms.append(km_value)
            except:
                continue
        
        return sum(kms) / len(kms) if kms else 0.0
    
    # NO CREAR HOJA RESUMEN AUTOMATICAMENTE
    def create_summary_sheet(self) -> bool:
        """DESHABILITADO: No crear hoja resumen automáticamente"""
        # Comentado para evitar crear hojas adicionales
        self.logger.info("Creación de hoja resumen deshabilitada según requerimientos")
        return True
        
        # El código original se mantiene comentado por si se necesita en el futuro
        """
        try:
            summary_sheet_name = "_RESUMEN_GENERAL"
            # ... resto del código comentado
        except Exception as e:
            self.logger.error(f"Error creando hoja resumen: {e}")
            return False
        """
    
    def get_model_sheet_names(self) -> List[str]:
        """Obtener nombres de todas las hojas de modelos"""
        try:
            all_sheets = self.spreadsheet.worksheets()
            return [sheet.title for sheet in all_sheets if not sheet.title.startswith("_")]
        except Exception as e:
            self.logger.error(f"Error obteniendo nombres de hojas: {e}")
            return []
    
    def delete_sheet(self, sheet_name: str) -> bool:
        """Eliminar una hoja específica"""
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self.spreadsheet.del_worksheet(worksheet)
            self.logger.info(f"Hoja eliminada: {sheet_name}")
            return True
        except gspread.WorksheetNotFound:
            self.logger.warning(f"Hoja no encontrada para eliminar: {sheet_name}")
            return False
        except Exception as e:
            self.logger.error(f"Error eliminando hoja {sheet_name}: {e}")
            return False

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def test_google_sheets_connection(sheet_id: str = None) -> bool:
    """Función de prueba para verificar la conexión a Google Sheets"""
    try:
        print("Probando conexión a Google Sheets...")
        
        manager = GoogleSheetsManager(sheet_id=sheet_id)
        success = manager.test_connection()
        
        if success:
            print("Conexión a Google Sheets exitosa")
            
            # Mostrar información adicional
            sheets = manager.get_model_sheet_names()
            print(f"Hojas encontradas: {len(sheets)}")
            for sheet in sheets[:5]:  # Mostrar solo las primeras 5
                print(f"   • {sheet}")
            if len(sheets) > 5:
                print(f"   ... y {len(sheets) - 5} más")
        else:
            print("Error en la conexión a Google Sheets")
        
        return success
        
    except Exception as e:
        print(f"Error probando conexión: {e}")
        return False

if __name__ == "__main__":
    # Prueba de conexión
    test_google_sheets_connection()
