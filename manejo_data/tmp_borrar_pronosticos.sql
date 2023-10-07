-- !preview conn=DBI::dbConnect(RSQLite::SQLite(), "G:/Mi unidad/casos_uso_analitica/fomento_ganadero/pronostico_leche_cruda/data/db_datos_modelos.db")

-- delete from

select * from
consolidado_pronosticos
where
nombre_pronostico = 'Pronostico datos 2023-07' and
fecha_proceso = '2023-10-03 11:17:14'