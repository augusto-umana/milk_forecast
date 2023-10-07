options(tidyverse.quiet = TRUE)
library(tidyverse)
library(lubridate)
library(timetk)
library(DBI)
library(RSQLite)

setwd("G:/Mi unidad/casos_uso_analitica/fomento_ganadero/pronostico_leche_cruda")

config <- configr::read.config(file = "config/config.json")

#Carga funciones de pronostico
source(paste0(config$config_base$directorio_aplicacion, 
              config$config_base$directorio_codigo_pronostico,
              config$config_base$funciones_pronostico))

acopio_total <- obtener_datos(fecha_fin = "2023-07-30", config = config)

consolidado_forecast <- obtener_consolidado_forecast(config) %>% 
  mutate(nombre_pronostico =  str_replace(string = nombre_pronostico,
                                          pattern = "Pronostico datos", 
                                          replacement = "Pronóstico"))

consolidado <- 
  acopio_total %>% 
  rename(litros_mes = litros_acopio_formal,
         litros_promedio_dia = litros_diarios_acopio_formal) %>% 
  mutate(nombre_pronostico = "Dato Real") %>% 
  bind_rows(
    consolidado_forecast %>% 
      select(
        fecha,
        litros_mes = volumen_mensual,
        litros_promedio_dia = volumen_promedio_dia,
        nombre_pronostico
      ))

errores_pronostico <- 
  acopio_total %>% 
  inner_join(consolidado_forecast, by = "fecha", multiple = "all") %>% 
    mutate(error_litr_mes = volumen_mensual - litros_acopio_formal,
           error_litr_mes_prc = volumen_mensual / litros_acopio_formal - 1,
           error_litr_promedio_dia = volumen_promedio_dia - litros_diarios_acopio_formal,
           error_litr_promedio_dia_prc = volumen_promedio_dia / litros_diarios_acopio_formal - 1)
  
nombre_pronosticos <- 
  consolidado %>% 
  group_by(nombre_pronostico) %>% 
  summarise(fecha_inicio = min(fecha),
            fecha_fin    = max(fecha)) %>% 
  ungroup()

  
  
rm(acopio_total, consolidado_forecast)
