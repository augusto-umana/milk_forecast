library(tidyverse)

renombrar_archivo_resultados <- function(nombre_archivo){
  # Datos en el directorio data/procesada
  ruta_archivo <- paste0("data/procesada/", nombre_archivo)

  #Fecha y hora Actual (Reemplaza ":" y (espacio) por -)
  fecha_texto <- str_replace_all(string = now(), pattern = ":| ",
                                 replacement = "-")

  nuevo_nombre <- paste0("data/procesada/",
                         str_replace(string = nombre_archivo,
                              pattern = "[.]",
                              replacement = paste0("_", fecha_texto, "_.")))

  if (file.exists(ruta_archivo)){

    file.rename(from = ruta_archivo, to = nuevo_nombre)
  }
}