obtener_datos <- function(fecha_fin="2022-12-31", config){
  #Consulta datos desde base de datos
  con <- dbConnect(drv = SQLite(),paste0(config$config_base$directorio_aplicacion, 
                                         config$config_base$directorio_basedatos,
                                         config$config_base$base_datos))
  
  
  nacional_acopio_leche <- 
    dbReadTable(conn = con, name = "recoleccion_nacional_leche") %>% 
    mutate(fecha = as.Date(fecha),
           anio = lubridate::year(fecha),
           mes = lubridate::month(fecha))
  dbDisconnect(con)
  
  
  #Datos historicos acopio formal
  acopio_formal <- 
    nacional_acopio_leche %>% 
    dplyr::filter(fecha >= as.Date("2010-01-01") &
                    fecha <= as.Date(fecha_fin)  ) %>% 
    group_by(fecha) %>% 
    summarise(precio_promedio_acopio_formal = weighted.mean(x = precio, w = volumen_recoleccion),
              litros_acopio_formal = sum(volumen_recoleccion)) %>% 
    ungroup() %>% 
    mutate(litros_diarios_acopio_formal = litros_acopio_formal/days_in_month(fecha)) 
  
 return(acopio_formal)
}

obtener_consolidado_forecast <- function(config){
  con <- dbConnect(drv = SQLite(),paste0(config$config_base$directorio_aplicacion, 
                                         config$config_base$directorio_basedatos,
                                         config$config_base$base_datos))
  
  
  forecasts <- 
    dbReadTable(conn = con, name = "consolidado_pronosticos") %>% 
    mutate(fecha = as.Date(fecha))
  dbDisconnect(con)
  
  return(forecasts)
  
}
  
leer_actualizar_modelo_acopio <- function(datos, config, tipo_modelo ="Mensual"){
  # Para leer el archivo correcto busca en los parametros de configuracion
  # Si es el modelo Mensual consulta el parametro "modelo_mensual"
  # de lo contrario lee el modelo diario
  if (tipo_modelo =="Mensual"){
    nombre_modelo <- config$modelos_pronostico$modelo_mensual
  } else{
    nombre_modelo <- config$modelos_pronostico$modelo_promedio_diario
  }
  #Ruta completa donde se encuentran los modelos
  ruta_modelo <- paste0(
    config$config_base$directorio_aplicacion,
    config$modelos_pronostico$directorio_modelos,
    nombre_modelo)
  
  #Lee el modelo
  modelo <- modelo_pronostico_promedio_diario <- readRDS(file = ruta_modelo)
  
  #Actualiza el modelo con los últimos datos dipsonibles
  modelo_actualizado <- 
    modelo %>% 
    modeltime_refit(datos)
  
  #Devuelve el modelo actualizado
  return(modelo_actualizado)
}

hacer_pronostico <- function(datos, tipo_pronostico, config){
  # Lee archivo de modelo adecuado a la prediccion que se quiere hacer:
  # Tipo_Pronostio: Mensual o Diario
  modelo <- leer_actualizar_modelo_acopio(
    datos = datos, 
    tipo_modelo = tipo_pronostico,
    config = config)
  
  #Hace forecast, quita los registros que no sean prediccion
  resultados <- 
    modelo %>% 
    modeltime_forecast(h=13, 
                       new_data = datos, 
                       actual_data = datos) %>% 
    dplyr::filter(.key == "prediction") %>% 
    select(fecha  =.index,
           volumen = .value)
  
  return(resultados)
  
}

pronosticar_desde_fecha <- function(fecha_fin, nombre_pronostico, config){
  #Get data
  acopio_formal <- obtener_datos(fecha_fin = fecha_fin, 
                                 config = config)
  
  # Hacer pronosticos
  message("Haciendo pronóstico mensual")
  resultados_mensual <- 
    hacer_pronostico(datos = acopio_formal, 
                     tipo_pronostico = "Mensual",
                     config = config) %>% 
    rename(volumen_mensual = volumen)
  
  message("Haciendo pronóstico diario")
  resultados_diario <- 
    hacer_pronostico(datos = acopio_formal, 
                     tipo_pronostico = "Diario", 
                     config = config) %>% 
    rename(volumen_promedio_dia = volumen)
  
  message("Creando DF total")
  # Union Dataset
  df_final <- inner_join(
    x = resultados_mensual ,
    y = resultados_diario,
    by = "fecha") %>% 
    mutate(
      nombre_pronostico = nombre_pronostico,
      fecha = as.character(fecha),
      fecha_proceso = as.character(Sys.time())
    )
  
  grafico_mensual <- 
  df_final |> 
    mutate(fecha = as.Date(fecha)) |> 
  ggplot(aes(x=fecha, y = volumen_mensual))+
    geom_line()+
    geom_point()+
    ggtitle("Pronóstico mensual leche")+
    theme_bw()
  
  print(grafico_mensual)
  
  message("Guardando resultado")
  # Gaurda resultados en base de datos
  resultado <- guardar_pronostico(df_final, config)
  
  
  return(resultado)
}

guardar_pronostico <- function(df_guardar, config){
  con <- dbConnect(drv = SQLite(),paste0(config$config_base$directorio_aplicacion, 
                                         config$config_base$directorio_basedatos,
                                         config$config_base$base_datos))
 resultado <-  dbWriteTable(conn = con, 
               name = "consolidado_pronosticos",
               value = df_guardar,
               overwrite = FALSE,
               append = TRUE)
  
  dbDisconnect(con)
  
  return(resultado)
}