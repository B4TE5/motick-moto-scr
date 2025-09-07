"""
================================================================================
                    GOOGLE SHEETS MANAGER - WALLAPOP MOTOS SCRAPER                    
================================================================================

Gestor centralizado para subir datos de motos a Google Sheets
Cada modelo se sube a una hoja separada con ordenamiento por rentabilidad

Autor: Carlos Peraza
Versión: 1.0
Fecha: Septiembre 2025
================================================================================
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from config import GOOGLE_SHEETS_CONFIG, get_sheet_id, is_github_actions

class GoogleSheetsManager:
    """Gestor para operaciones con Google Sheets"""
    
    def __init__(self, credentials_source: str = None, sheet_id: str = None):
        """
        Inicializar el gestor de Google Sheets
        
        Args:
            credentials_source: Fuente de credenciales ("ENV", ruta archivo, o JSON string)
            sheet_id: ID del Google Sheet
        """
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
            self.logger.info(f" Conectado a Google Sheet: {self.spreadsheet.title}")
        except Exception as e:
            self.logger.error(f" Error conectando a Google Sheet: {e}")
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
            self.logger.error(f" Error configurando credenciales: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Probar la conexión a Google Sheets"""
        try:
            title = self.spreadsheet.title
            worksheets = len(self.spreadsheet.worksheets())
            self.logger.info(f" Conexión exitosa a '{title}' ({worksheets} hojas)")
            return True
        except Exception as e:
            self.logger.error(f" Error probando conexión: {e}")
            return False
    
    def upload_modelo_data(self, 
                          modelo_key: str, 
                          df_motos: pd.DataFrame, 
                          sheet_name: str,
                          overwrite: bool = True) -> bool:
        """
        Subir datos de un modelo específico a su hoja correspondiente
        
        Args:
            modelo_key: Clave del modelo (ej: 'cb125r')
            df_motos: DataFrame con los datos de las motos ordenados por rentabilidad
            sheet_name: Nombre de la hoja en Google Sheets
            overwrite: Si sobrescribir la hoja existente
            
        Returns:
            bool: True si fue exitoso
        """
        try:
            self.logger.info(f" Subiendo {len(df_motos)} motos del modelo {modelo_key} a hoja '{sheet_name}'")
            
            # Obtener o crear la hoja
            worksheet = self._get_or_create_worksheet(sheet_name, len(df_motos) + 10, len(df_motos.columns) + 2)
            
            if overwrite:
                worksheet.clear()
            
            # Preparar datos con encabezados
            headers = df_motos.columns.tolist()
            data_rows = df_motos.values.tolist()
            
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
            self._add_metadata_to_sheet(worksheet, modelo_key, df_motos, len(all_data))
            
            self.logger.info(f" {len(df_motos)} motos subidas exitosamente a '{sheet_name}'")
            self.logger.info(f" URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f" Error subiendo modelo {modelo_key}: {e}")
            return False
    
    def _get_or_create_worksheet(self, sheet_name: str, rows: int, cols: int):
        """Obtener hoja existente o crear una nueva"""
        try:
            # Intentar obtener hoja existente
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self.logger.info(f" Usando hoja existente: {sheet_name}")
            return worksheet
        except gspread.WorksheetNotFound:
            # Crear nueva hoja
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name,
                rows=rows,
                cols=cols
            )
            self.logger.info(f" Nueva hoja creada: {sheet_name}")
            return worksheet
    
    def _add_metadata_to_sheet(self, worksheet, modelo_key: str, df_motos: pd.DataFrame, data_end_row: int):
        """Añadir metadatos al final de la hoja"""
        try:
            # Calcular estadísticas
            total_motos = len(df_motos)
            precio_medio = df_motos['Precio'].apply(self._extract_price_number).mean()
            km_medio = df_motos['Kilometraje'].apply(self._extract_km_number).mean()
            
            # Crear metadatos
            metadata_start = data_end_row + 3
            metadata = [
                ["═══ METADATOS ═══", "", "", ""],
                ["Modelo", modelo_key.upper(), "", ""],
                ["Última actualización", datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "", ""],
                ["Total motos encontradas", total_motos, "", ""],
                ["Precio medio", f"{precio_medio:.0f}€" if not pd.isna(precio_medio) else "N/A", "", ""],
                ["KM medio", f"{km_medio:.0f}" if not pd.isna(km_medio) else "N/A", "", ""],
                ["Origen", "Wallapop Scraper Automation", "", ""],
                ["Ordenamiento", "Por rentabilidad (mayor a menor)", "", ""],
                ["Estado", "Completado", "", ""]
            ]
            
            # Subir metadatos
            start_cell = f"A{metadata_start}"
            worksheet.update(start_cell, metadata)
            
        except Exception as e:
            self.logger.warning(f" Error añadiendo metadatos: {e}")
    
    def _extract_price_number(self, price_text: str) -> float:
        """Extraer número del precio para cálculos"""
        if pd.isna(price_text) or not price_text:
            return 0.0
        
        import re
        price_str = str(price_text)
        numbers = re.findall(r'\d+', price_str.replace('.', '').replace(',', ''))
        
        if numbers:
            return float(numbers[0])
        return 0.0
    
    def _extract_km_number(self, km_text: str) -> float:
        """Extraer número del kilometraje para cálculos"""
        if pd.isna(km_text) or not km_text:
            return 0.0
        
        import re
        km_str = str(km_text)
        numbers = re.findall(r'\d+', km_str.replace('.', '').replace(',', ''))
        
        if numbers:
            return float(numbers[0])
        return 0.0
    
    def create_summary_sheet(self) -> bool:
        """Crear hoja resumen con estadísticas de todos los modelos"""
        try:
            summary_sheet_name = "_RESUMEN_GENERAL"
            worksheet = self._get_or_create_worksheet(summary_sheet_name, 50, 10)
            worksheet.clear()
            
            # Obtener estadísticas de todas las hojas
            all_sheets = self.spreadsheet.worksheets()
            model_sheets = [sheet for sheet in all_sheets if not sheet.title.startswith("📊")]
            
            summary_data = [
                ["═══ RESUMEN GENERAL - WALLAPOP MOTOS ═══", "", "", "", "", "", "", "", ""],
                ["Última actualización", datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "", "", "", "", "", "", ""],
                ["Total modelos monitoreados", len(model_sheets), "", "", "", "", "", "", ""],
                ["", "", "", "", "", "", "", "", ""],
                ["MODELO", "TOTAL MOTOS", "PRECIO MEDIO", "KM MEDIO", "MÁS BARATA", "MÁS CARA", "MENOS KM", "MÁS KM", "ÚLTIMA ACTUALIZACIÓN"]
            ]
            
            # Analizar cada hoja de modelo
            for sheet in model_sheets:
                try:
                    # Obtener datos de la hoja
                    data = sheet.get_all_values()
                    if len(data) < 2:  # Solo encabezados o vacía
                        continue
                    
                    df = pd.DataFrame(data[1:], columns=data[0])
                    
                    if len(df) == 0:
                        continue
                    
                    # Calcular estadísticas
                    precios = df['Precio'].apply(self._extract_price_number)
                    kms = df['Kilometraje'].apply(self._extract_km_number)
                    
                    precios_validos = precios[precios > 0]
                    kms_validos = kms[kms > 0]
                    
                    model_stats = [
                        sheet.title,
                        len(df),
                        f"{precios_validos.mean():.0f}€" if len(precios_validos) > 0 else "N/A",
                        f"{kms_validos.mean():.0f}" if len(kms_validos) > 0 else "N/A",
                        f"{precios_validos.min():.0f}€" if len(precios_validos) > 0 else "N/A",
                        f"{precios_validos.max():.0f}€" if len(precios_validos) > 0 else "N/A",
                        f"{kms_validos.min():.0f}" if len(kms_validos) > 0 else "N/A",
                        f"{kms_validos.max():.0f}" if len(kms_validos) > 0 else "N/A",
                        datetime.now().strftime("%d/%m/%Y %H:%M")
                    ]
                    
                    summary_data.append(model_stats)
                    
                except Exception as e:
                    self.logger.warning(f" Error procesando hoja {sheet.title}: {e}")
                    continue
            
            # Subir datos del resumen
            worksheet.update(summary_data)
            
            self.logger.info(f" Hoja resumen creada/actualizada: {summary_sheet_name}")
            return True
            
        except Exception as e:
            self.logger.error(f" Error creando hoja resumen: {e}")
            return False
    
    def get_model_sheet_names(self) -> List[str]:
        """Obtener nombres de todas las hojas de modelos"""
        try:
            all_sheets = self.spreadsheet.worksheets()
            return [sheet.title for sheet in all_sheets if not sheet.title.startswith("📊")]
        except Exception as e:
            self.logger.error(f" Error obteniendo nombres de hojas: {e}")
            return []
    
    def delete_sheet(self, sheet_name: str) -> bool:
        """Eliminar una hoja específica"""
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self.spreadsheet.del_worksheet(worksheet)
            self.logger.info(f" Hoja eliminada: {sheet_name}")
            return True
        except gspread.WorksheetNotFound:
            self.logger.warning(f" Hoja no encontrada para eliminar: {sheet_name}")
            return False
        except Exception as e:
            self.logger.error(f" Error eliminando hoja {sheet_name}: {e}")
            return False

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def test_google_sheets_connection(sheet_id: str = None) -> bool:
    """
    Función de prueba para verificar la conexión a Google Sheets
    
    Args:
        sheet_id: ID del Google Sheet (opcional)
        
    Returns:
        bool: True si la conexión es exitosa
    """
    try:
        print("🔍 Probando conexión a Google Sheets...")
        
        manager = GoogleSheetsManager(sheet_id=sheet_id)
        success = manager.test_connection()
        
        if success:
            print(" Conexión a Google Sheets exitosa")
            
            # Mostrar información adicional
            sheets = manager.get_model_sheet_names()
            print(f" Hojas encontradas: {len(sheets)}")
            for sheet in sheets[:5]:  # Mostrar solo las primeras 5
                print(f"   • {sheet}")
            if len(sheets) > 5:
                print(f"   ... y {len(sheets) - 5} más")
        else:
            print(" Error en la conexión a Google Sheets")
        
        return success
        
    except Exception as e:
        print(f" Error probando conexión: {e}")
        return False

if __name__ == "__main__":
    # Prueba de conexión
    test_google_sheets_connection()