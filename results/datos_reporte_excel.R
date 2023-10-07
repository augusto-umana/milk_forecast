
#Ejecuta extracción datos
source("results/datos_reporte_powerbi.R")

#Guarda resultados en archivos de texto

write_excel_csv2(x = consolidado, 
                 file = paste0("results/consolidado_forecast.txt"),na = "")


write_excel_csv2(x = errores_pronostico, 
                 file = paste0("results/errores_pronostico.txt"),na = "")
