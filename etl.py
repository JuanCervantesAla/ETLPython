import pandas as pd
import numpy as np
import unicodedata as unicodedata
import json

class ETLProcessor:
    def __init__(self, excel_file="dirty_dataset.xlsx", csv_file="dirty_dataset.csv", json_file="dirty_dataset.json"):
        self.excel_file = excel_file
        self.csv_file = csv_file
        self.json_file = json_file
        self.data = None

    #############################Manuel#############################
    def load_data(self, file_type):
        # Carga los datos segun el tipo de archivo seleccionado
        try:
            if file_type == "excel":
                sheets = pd.read_excel(self.excel_file, sheet_name=None, engine='openpyxl')
                self.data = pd.concat(sheets.values(), ignore_index=True)
            elif file_type == "csv":
                self.data = pd.read_csv(self.csv_file, encoding='utf-8')
            elif file_type == "json":
                # Cargar el JSON como diccionario y luego concatenar cada lista en un DataFrame
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                # Crear un DataFrame para cada clave y concatenarlos
                df_list = [pd.DataFrame(records) for key, records in json_data.items()]
                self.data = pd.concat(df_list, ignore_index=True)
            print(f"Datos cargados exitosamente desde {file_type.upper()}.\n")
        except Exception as e:
            print(f"Error al cargar el archivo {file_type}: {e}")

    def remove_duplicates(self):
        # Elimina registros duplicados del Dataset
        if self.data is None or self.data.empty:
            print("No hay datos cargados o el Dataset esta vacio")
            return
        rows_before = len(self.data)
        self.data.drop_duplicates(inplace=True)
        rows_after = len(self.data)
        print(f"Registros eliminados: {rows_before - rows_after}")
        print("Registros duplicados eliminados exitosamente")

    def export_data(self, file_name, file_format):
        # Exporta los datos en formato Excel, CSV o JSON
        if self.data is None or self.data.empty:
            print("No hay datos para exportar")
            return
        try:
            if file_format == 'excel':
                self.data.to_excel(f"{file_name}.xlsx", index=False, sheet_name='Datos', engine='openpyxl')
            elif file_format == 'csv':
                self.data.to_csv(f"{file_name}.csv", index=False, encoding='utf-8')
            elif file_format == 'json':
                self.data.to_json(f"{file_name}.json", orient='records', force_ascii=False)
            elif file_format == 'sql':
                # Generar script SQL
                with open(f"{file_name}.sql", "w", encoding="utf-8") as sql_file:
                    # Crear tabla con columnas basadas en el DataFrame
                    columns = ",\n    ".join(f"{col} TEXT" for col in self.data.columns)
                    sql_file.write(f"CREATE TABLE datos (\n    {columns}\n);\n\n")
                    
                    # Insertar datos
                    for _, row in self.data.iterrows():
                        values = ", ".join(f"'{str(value).replace("'", "''")}'" if pd.notna(value) else "NULL" for value in row)
                        sql_file.write(f"INSERT INTO datos VALUES ({values});\n")
            print(f"Datos exportados exitosamente a {file_name}.{file_format}")
        except Exception as e:
            print(f"Error al exportar datos: {e}")
    
    def fill_missing_values_all_interpolate(self, metodo='linear'):
        # Rellena los valores nulos en todas las columnas numéricas usando interpolación.
        # Se calcula la media entre los valores vecinos para cada columna numérica.
        if self.data is None or self.data.empty:
            print("No hay datos cargados.")
            return
        numeric_cols = self.data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            try:
                nulos_antes = self.data[col].isnull().sum()
                self.data[col] = self.data[col].interpolate(method=metodo)
                nulos_despues = self.data[col].isnull().sum()
                #print(f"Columna '{col}': nulos antes: {nulos_antes}, nulos después: {nulos_despues} (rellenados con '{metodo}').")
            except Exception as e:
                print(f"Error al interpolar valores en '{col}': {e}")

    def fill_missing_values_all_ffill(self):
        
        # Rellena los valores nulos en todas las columnas utilizando el método ffill,
        # propagando el último valor no nulo hacia adelante.
        
        if self.data is None or self.data.empty:
            print("No hay datos cargados.")
            return
        for col in self.data.columns:
            try:
                nulos_antes = self.data[col].isnull().sum()
                self.data[col] = self.data[col].ffill()
                nulos_despues = self.data[col].isnull().sum()
                #print(f"Columna '{col}': nulos antes: {nulos_antes}, nulos después: {nulos_despues} (rellenados con ffill).")
            except Exception as e:
                print(f"Error al aplicar ffill en '{col}': {e}")

    def fill_missing_values_all_bfill(self):

        # Rellena los valores nulos en todas las columnas utilizando el método bfill,
        # propagando el primer valor válido hacia atrás.

        if self.data is None or self.data.empty:
            print("No hay datos cargados.")
            return
        for col in self.data.columns:
            try:
                nulos_antes = self.data[col].isnull().sum()
                self.data[col] = self.data[col].bfill()
                nulos_despues = self.data[col].isnull().sum()
                #print(f"Columna '{col}': nulos antes: {nulos_antes}, nulos después: {nulos_despues} (rellenados con bfill).")
            except Exception as e:
                print(f"Error al aplicar bfill en '{col}': {e}")

    ########################################Sergio#########################
    def normalize_dates(self):
        
        # Normaliza formatos de fecha a 'YYYY-MM-DD' en todo el Dataset.
        # Reemplaza formatos incorrectos encontrados.
        
        if self.data is None or self.data.empty:
            print("No hay datos cargados o el Dataset esta vacio")
            return
        
        date_patterns = [
            # Mapeo de patrones incorrectos a correctos
            (r'\b(\d{1,2})-(\d{1,2})-(\d{4})\b', r'\3-\1-\2'),  # MM-DD-YYYY → YYYY-MM-DD
            (r'\b(\d{4})/(\d{1,2})/(\d{1,2})\b', r'\1-\2-\3'),  # YYYY/MM/DD → YYYY-MM-DD
            (r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b', r'\3-\1-\2'),  # MM/DD/YYYY → YYYY-MM-DD
            (r'\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b', r'\3-\1-\2')  # DD.MM.YYYY → YYYY-MM-DD
        ]
        
        for col in self.data.columns:
            if self.data[col].dtype == 'object':
                try:
                    for pattern, replacement in date_patterns:
                        self.data[col] = self.data[col].replace(pattern, replacement, regex=True)
                    # Intentar convertir a datetime para validar
                    pd.to_datetime(self.data[col], errors='raise')
                    print(f"Columna '{col}': Formatos de fecha normalizados a 'YYYY-MM-DD'")
                except (ValueError, TypeError):
                    continue

    def convert_numeric_columns(self):

        # Convierte columnas con valores numéricos a su tipo adecuado (int o float).

        if self.data is None or self.data.empty:
            print("No hay datos cargados o el Dataset esta vacio")
            return
        
        for col in self.data.columns:
            if self.data[col].dtype == 'object':
                # Intentar convertir a numérico
                try:
                    # Primero eliminamos espacios y comas en números (ej: "1 000" o "1,000")
                    temp_series = self.data[col].str.replace(r'[\s,]', '', regex=True)
                    # Intentar convertir a float
                    converted = pd.to_numeric(temp_series, errors='raise')
                    
                    # Verificar si todos los valores son enteros
                    if (converted % 1 == 0).all():
                        self.data[col] = converted.astype(int)
                        print(f"Columna '{col}' convertida a tipo entero")
                    else:
                        self.data[col] = converted.astype(float)
                        print(f"Columna '{col}' convertida a tipo flotante")
                except (ValueError, TypeError, AttributeError):
                    continue

    def remove_accents(self):

        # Elimina tildes y caracteres acentuados de todas las cadenas de texto en el Dataset.

        if self.data is None or self.data.empty:
            print("No hay datos cargados o el Dataset esta vacio")
            return
        
        def remove_accents_str(x):
            if isinstance(x, str):
                return ''.join(c for c in unicodedata.normalize('NFD', x) 
                             if unicodedata.category(c) != 'Mn')
            return x
        
        self.data = self.data.map(remove_accents_str)
        print("Tildes y caracteres acentuados eliminados de todo el Dataset")


    #################################Noel####################################
    def convertir_reservation_date(self):
        
        #Convierte 'reservation_status_date' a datetime usando pd.to_datetime (errors='coerce').
        #Requiere que el dataset esté cargado y que la columna exista.
        #Actualiza self.data con la columna convertida.
        
        if self.data is None or self.data.empty:
            print("No hay datos cargados.")
            return
        if "reservation_status_date" not in self.data.columns:
            print("La columna 'reservation_status_date' no existe en el dataset.")
            return
        try:
            self.data["reservation_status_date"] = pd.to_datetime(
                self.data["reservation_status_date"],
                errors='coerce'
            )
            print("Columna 'reservation_status_date' convertida a datetime.")
        except Exception as e:
            print(f"Error al convertir 'reservation_status_date': {e}")

    def extraer_componentes_reservation_date(self, sufijo=True):
        
        #Extrae día, mes, año, día de la semana y trimestre de 'reservation_status_date'
        #y agrega nuevas columnas (con prefijo si sufijo=True).
        #Requiere que la columna sea datetime.
        
        if self.data is None or self.data.empty:
            print("No hay datos cargados.")
            return
        if "reservation_status_date" not in self.data.columns:
            print("La columna 'reservation_status_date' no existe en el dataset.")
            return
        if not pd.api.types.is_datetime64_any_dtype(self.data["reservation_status_date"]):
            print("La columna 'reservation_status_date' no es de tipo datetime.")
            return
        try:
            base_name = "reservation_status_date_" if sufijo else ""
            self.data[f"{base_name}dia"] = self.data["reservation_status_date"].dt.day.fillna(-1).astype(int)
            self.data[f"{base_name}mes"] = self.data["reservation_status_date"].dt.month.fillna(-1).astype(int)
            self.data[f"{base_name}año"] = self.data["reservation_status_date"].dt.year.fillna(-1).astype(int)
            self.data[f"{base_name}dia_semana"] = self.data["reservation_status_date"].dt.dayofweek.fillna(-1).astype(int)
            self.data[f"{base_name}trimestre"] = self.data["reservation_status_date"].dt.quarter.fillna(-1).astype(int)
            print("Componentes de fecha extraídos de 'reservation_status_date'.")
        except Exception as e:
            print(f"Error al extraer componentes de 'reservation_status_date': {e}")

    def extraer_nombre_dia(self):
    
        # Agrega la columna 'nombre_dia' con el nombre del día extraído de 
        #'reservation_status_date'. Se asegura de que la columna sea datetime.
        
        if self.data is None or self.data.empty:
            print("No hay datos cargados.")
            return
        if "reservation_status_date" not in self.data.columns:
            print("La columna 'reservation_status_date' no existe en el dataset.")
            return
        try:
            # Asegurarse de que la columna es datetime
            if not pd.api.types.is_datetime64_any_dtype(self.data["reservation_status_date"]):
                self.data["reservation_status_date"] = pd.to_datetime(self.data["reservation_status_date"], errors='coerce')
            # Extraer el nombre del día 
            self.data["nombre_dia"] = self.data["reservation_status_date"].dt.day_name(locale='es_ES')
            print("Columna 'nombre_dia' agregada exitosamente.")
        except Exception as e:
            print(f"Error al agregar 'nombre_dia': {e}")

    #################################Juanjo##################################################
    def analizarReservaciones(self, group_column='customer_type', metric_column='adr', new_column_name='analisis_reservacion'):
        #Validar si los datos estan vacios o no
        if self.data is None:
            return None
            
        #Validar si las columnas existen
        required_columns = [group_column, metric_column]
        missing_cols = [col for col in required_columns if col not in self.data.columns]
        
        #Si hay columnas faltantes regresa
        if missing_cols:
            return None
        
        try:
            #Hacer analisis entre columna  y el adr
            analysis = self.data.groupby(group_column)[metric_column].transform('mean').round(2)
            
            #Agregamos como nueva columna
            self.data[new_column_name] = analysis
            
            return self.data
        
        except Exception as e:
            print(f"Error al analizar reservaciones: {str(e)}")
            return None
        

    def getTotalCost(self):
        #Crea nuevas columnas de  las noches totales, huespedes totales y costos totales
        
        #Columnas que van a realizar las operaciones
        required_columns = ['stays_in_weekend_nights', 'stays_in_week_nights', 'adr', 'adults', 'children','babies']
        
        #Si los datos son vacios regresa
        if self.data is None:
            return None
            
        #Verifica de las columnas si hay columnas faltantes
        missing_cols = [col for col in required_columns if col not in self.data.columns]
        #Checa el booleano y retorna si hay faltantes
        if missing_cols:
            return None
        
        try:
            #Calcula las noches totales sumando las del fin de semana y las de entre semana
            self.data['total_nights'] = self.data['stays_in_weekend_nights'] + self.data['stays_in_week_nights']
            
            #Calcula el total de huespedes sumando adultos, ninos y bebes
            self.data['total_guests'] = self.data['adults'] + self.data['children'] + self.data['babies']
            
            #Calcula el costo total tomando el total de las noches por la tarifa diaria
            self.data['total_cost'] = self.data['total_nights'] * self.data['adr']
            
            #Return the data head
            return self.data[['total_nights', 'total_guests', 'total_cost']].head()
            
        except Exception as e:
            print(f"Error al calcular costos: {str(e)}")
            return None
    

    def agregarColumnaCancelaciones(self):
        #Anadir columna que identifica reservas canceladas con alta anticipacion

        # Verificar datos cargados
        if self.data is None:
            return None
        
        try:
            #Crear nueva columna simple
            self.data['cancelacion_alto_riesgo'] = 0 
            
            #Identificar reservas de alto riesgo
            mask = (self.data['is_canceled'] == 1) & (self.data['lead_time'] > 90)
            self.data.loc[mask, 'cancelacion_alto_riesgo'] = 1
            
            return self.data
        #Regresa si no funciona
        except Exception as e:
            print(f"Error al agregar columna: {str(e)}")
            return None


    #################################Diego###################################################
    def categorizar_lead_time(self, columna='lead_time'):
        # Crea una nueva columna 'lead_time_category' en el dataset, segmentando
        # la columna 'lead_time' en tres categorías: 'Corto', 'Medio' y 'Largo'.

        # Parámetros:
        #   - self: Dataset que contiene la columna 'lead_time'.
        #   - columna: Nombre de la columna a categorizar (por defecto, 'lead_time').

        # Retorna:
        #   Dataset con la nueva columna 'lead_time_category'.
        
        # Aqui se definen los intervalos y etiquetas para poder categorizar
        bins = [0, 10, 30, self.data[columna].max()]
        etiquetas = ['Corto', 'Medio', 'Largo']
        self.data['lead_time_category'] = pd.cut(self.data[columna], bins=bins, labels=etiquetas, include_lowest=True)
        return self.data
    
    def calcular_promedio_movil_lead_time(self, df, ventana=7):
        # Calcula el promedio móvil del lead_time usando una ventana definida.
        # Ordena el Dataset por 'reservation_status_date' y utiliza el método rolling().
        #Objetivo: Esto permite observar tendencias en la anticipación de las reservaciones a lo largo del tiempo.
        
        # Parámetros:
        # - df: Dataset que contiene la columna 'lead_time' y 'reservation_status_date'.
        # - ventana: Tamaño de la ventana para el promedio móvil (por defecto, 7 registros).
        
        # Retorna:
        # Dataset con una nueva columna 'lead_time_movil' que contiene el promedio móvil.
        
        # Asegurar que 'reservation_status_date' es datetime y 'lead_time' es numérico
        df['reservation_status_date'] = pd.to_datetime(df['reservation_status_date'], errors='coerce')
        df['lead_time'] = pd.to_numeric(df['lead_time'], errors='coerce')
        
        # Ordenar el Dataset por fecha
        df = df.sort_values('reservation_status_date').reset_index(drop=True)
        
        # Calcular el promedio móvil usando rolling()
        df['lead_time_movil'] = df['lead_time'].rolling(window=ventana, min_periods=1).mean()
        
        return df

    def crear_total_huespedes(self, df):
        # Crea una nueva columna 'total_huespedes' utilizando eval(), que suma los valores de
        # 'adults', 'children' y 'babies'.
        
        # Parámetros:
        # - df: DataFrame que contiene las columnas 'adults', 'children' y 'babies'.
        
        # Retorna:
        # DataFrame con la nueva columna 'total_huespedes'.

        # Usar eval para crear la columna de forma rápida
        df = df.copy()  # Evitar modificar el DataFrame original
        
        # Convertir las columnas relevantes a numérico, usando 0 para los valores no convertibles
        for col in ['adults', 'children', 'babies']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df.eval('total_huespedes = adults + children + babies', inplace=True)
        return df


























































































































