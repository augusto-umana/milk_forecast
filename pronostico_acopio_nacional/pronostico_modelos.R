# Environment setup ----
# Configura librerías
library(tidyverse)
library(lubridate)
library(timetk)
library(tidymodels)
library(modeltime)
library(DBI)
library(RSQLite)

fecha_fin <- "2023-07-31"
nombre_pronostico <- "Pronostico datos 2023-07"

#Lee archivo de configuraciones
config <- configr::read.config(file = "config/config.json")

#Carga funciones de pronostico
source(paste0(config$config_base$directorio_aplicacion, 
              config$config_base$directorio_codigo_pronostico,
              config$config_base$funciones_pronostico))

resultado <- pronosticar_desde_fecha(fecha_fin = fecha_fin, 
                                     nombre_pronostico = nombre_pronostico, 
                                     config = config)
