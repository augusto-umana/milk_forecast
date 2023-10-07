-- !preview conn=DBI::dbConnect(RSQLite::SQLite(), "G:/Mi unidad/casos_uso_analitica/fomento_ganadero/pronostico_leche_cruda/data/db_datos_modelos.db")

SELECT 
  nombre_pronostico, 
  fecha_proceso, 
  count(*) as cant 
FROM 
  consolidado_pronosticos 
GROUP BY
  nombre_pronostico, 
  fecha_proceso
ORDER BY 
  nombre_pronostico, 
  fecha_proceso