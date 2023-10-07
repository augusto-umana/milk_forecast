#Pasos:
#Descargar Información
#Extraer archvios reporte
#Transformar datos leidos para generar un solo dataframe con información de Precio y Cantidad por Region

import requests
import json
from datetime import date
from datetime import datetime
import zipfile
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import CHAR, DATE, DECIMAL, INTEGER

#Configuracion
with open("config/config.json", "r") as f:
  config = json.load(f)

#*******************************************************************************
#* Cambiar método de acceso para descargar los archivos 
#* desde el sharepoint del Ministerio
#*******************************************************************************
#*
url_reporte_volumen = config["config_reporte_ministerio"]["url_reporte_volumen"]
url_reporte_precio = config["config_reporte_ministerio"]["url_reporte_precio"]

direccion_base_datos = config["config_base"]["directorio_basedatos"] + \
                        config["config_base"]["base_datos"]

directorio_reportes = config["config_base"]["directorio_aplicacion"] + \
                      config["config_reporte_ministerio"]["directorio_reportes"]

nombre_departamentos = pd.read_excel(io = config["config_base"]["directorio_aplicacion"] + \
                      config["config_reporte_ministerio"]["archivo_nombre_departamentos"])  

#Obtiene reporte desde url ministerio

# ** Archivo volumen
archivo_volumen = directorio_reportes + config["config_reporte_ministerio"]["nombre_archivo_volumen"] + date.today().strftime("%Y_%m_%d")+".xlsx"
resultado_download_volumen = requests.get(url_reporte_volumen)
open(archivo_volumen, "wb").write(resultado_download_volumen.content)

# ** ArchivoPrecios
archivo_precios = directorio_reportes + config["config_reporte_ministerio"]["nombre_archivo_precio"] + date.today().strftime("%Y_%m_%d")+".xlsx"
resultado_download_precio = requests.get(url_reporte_precio)
open(archivo_precios, "wb").write(resultado_download_precio.content)

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

# Fin
