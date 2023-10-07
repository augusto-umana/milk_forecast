library(tidyverse)
library(DBI)
library(RSQLite)


borrar_fecha_inicio <- "2023-05-01"
borrar_fecha_fin <- "2023-07-31"

borrar_nombre_pronostico <- "Pronostico datos 2023-07"

#Lee archivo de configuraciones
config <- configr::read.config(file = "config/config.json")

#Conecta a base de datos
con <- dbConnect(drv = SQLite(),paste0(config$config_base$directorio_aplicacion, 
                                                        config$config_base$directorio_basedatos,
                                                        config$config_base$base_datos))
                 

dbListTables(con)

consolidado_pronosticos <- tbl(con, "consolidado_pronosticos")
recoleccion_nacional_leche <- tbl(con,"recoleccion_nacional_leche")

#*********** Borrar pronostico ********** ----
cmd_borrar_pronosticos <- paste0("DELETE FROM consolidado_pronosticos WHERE nombre_pronostico = '", 
                                 borrar_nombre_pronostico, "'")

consolidado_pronosticos |> count(nombre_pronostico) |> collect() |> print(n=1000)

resultado <- dbExecute(con, cmd_borrar_pronosticos)

consolidado_pronosticos |> count(nombre_pronostico) |> collect() |> print(n=1000)

# Borrar datos reales pronostico


#*********** Borrar datos ********** ----

cmd_borrar_datos_recoleccion <- paste0("DELETE FROM recoleccion_nacional_leche WHERE fecha >= '",
                           borrar_fecha_inicio,
                           "' AND fecha <= '",
                           borrar_fecha_fin, "'")

recoleccion_nacional_leche |> count(fecha) |> collect() |> print(n=1000)
resultado <- dbExecute(con, cmd_borrar_datos_recoleccion)
recoleccion_nacional_leche |> count(fecha) |> collect() |> print(n=1000)

dbDisconnect(con)
