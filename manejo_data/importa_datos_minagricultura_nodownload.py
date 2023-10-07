#Pasos:
#Descargar Información
#Extraer archvios reporte
#Transformar datos leidos para generar un solo dataframe con información de Precio y Cantidad por Region

import json
from datetime import date
from datetime import datetime
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import CHAR, DATE, DECIMAL, INTEGER

#Configuracion
fecha_datos_archivo = "2023_10_06"

with open("G:/Mi unidad/casos_uso_analitica/fomento_ganadero/pronostico_leche_cruda/config/config.json", "r") as f:
  config = json.load(f)

direccion_base_datos = config["config_base"]["directorio_basedatos"] + \
                        config["config_base"]["base_datos"]

directorio_reportes = config["config_base"]["directorio_aplicacion"] + \
                      config["config_reporte_ministerio"]["directorio_reportes"]

nombre_departamentos = pd.read_excel(io = config["config_base"]["directorio_aplicacion"] + \
                      config["config_reporte_ministerio"]["archivo_nombre_departamentos"])



# ** Archivo volumen
archivo_volumen = directorio_reportes + config["config_reporte_ministerio"]["nombre_archivo_volumen"] + fecha_datos_archivo+".xlsx"

# ** ArchivoPrecios
archivo_precios = directorio_reportes + config["config_reporte_ministerio"]["nombre_archivo_precio"] + fecha_datos_archivo+".xlsx"

# ***** Información volumen recoleccion *****


df_volumen_acopio  = pd.read_excel(io=archivo_volumen,
                                     sheet_name = "VOLUMEN REGIONAL Y DEPTAL ", 
                                     skiprows = 3)
                                     
volumen_recoleccion = pd.melt(df_volumen_acopio.drop("Unnamed: 0", axis = 1), 
                              id_vars=["Región", "Departamento"],
                              var_name = "fecha", 
                              value_name = "volumen_recoleccion").\
                              query("(`Región` == 1 or `Región` == 2) and `volumen_recoleccion`!= 'nd'")
                              
volumen_recoleccion['fecha'] = volumen_recoleccion['fecha'].dt.date
volumen_recoleccion['volumen_recoleccion'] = volumen_recoleccion['volumen_recoleccion'].astype('float64')

# Cruza con df Nombre_Departamento para obtener el codigo y nombre estandar del departamento
volumen_recoleccion = volumen_recoleccion.merge(nombre_departamentos, 
                                    left_on = 'Departamento',
                                    right_on  ='Departamento_Recoleccion').\
                                    drop(columns = ["Departamento", 'Departamento_Recoleccion', 'Departamento_Precio'])

# ***** Información precios recoleccion ***** 


df_precio_productor = pd.read_excel(io = archivo_precios, 
                                sheet_name = "PRECIO ($) TOTAL DEP", skiprows = 3,
                                parse_dates = [1])
                                
precio_productor = pd.melt(df_precio_productor.drop("Unnamed: 0", axis = 1).dropna(),
                            id_vars = "PERIODO",
                            var_name = "Departamento", 
                            value_name = "precio").\
                            query("`precio` != 'nd'")
                            
precio_productor["fecha"] = pd.to_datetime(precio_productor["PERIODO"]).dt.date
precio_productor["precio"] = precio_productor["precio"].astype('float64')

# Cruza con df Nombre_Departamento para obtener el codigo y nombre estandar del departamento
precio_productor = precio_productor.merge(nombre_departamentos, 
                                    left_on = 'Departamento',
                                    right_on  ='Departamento_Precio').\
                                    drop(columns = ["Departamento", 'Departamento_Recoleccion', 'Departamento_Precio', 'nombre_departamento'])


# ***** Union datos recoleccion y volumen *****
total_datos = volumen_recoleccion.merge(precio_productor,
                              how = 'left',
                              left_on = ["fecha", "cod_depto_divipola"], 
                              right_on =["fecha", "cod_depto_divipola"]).\
                              drop(columns = "PERIODO")
#Conteo fechas
total_datos.groupby("fecha")["fecha"].count()

# ***** Copia informacion a base de datos *****
engine = create_engine("sqlite:///"+direccion_base_datos)

# Borra tabla anterior 
connection = engine.raw_connection()
cursor = connection.cursor()
command = "SELECT max(fecha) FROM recoleccion_nacional_leche;"

cursor.execute(command)

df_fechas_db = pd.DataFrame(cursor.fetchall())


datos_nuevos = total_datos[total_datos['fecha'] > datetime.strptime(df_fechas_db[0][0], '%Y-%m-%d').date()]

datos_nuevos.groupby("fecha")["volumen_recoleccion", "precio"].sum()
#Copia datos a tabla
datos_nuevos.to_sql('recoleccion_nacional_leche', 
                    engine, 
                    index = False, 
                    if_exists = 'append',
                    dtype={"index": INTEGER(),
                           "Región": INTEGER(),
                           "fecha": DATE(),
                           "volumen_recoleccion": DECIMAL(),
                           "precio": DECIMAL(),
                           "nombre_departamento": CHAR(),
                           "cod_depto_divipola": INTEGER()
                           })
connection.close()

# Fin
