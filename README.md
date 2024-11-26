# Introduction to Data Engineering

Para este reto, construirás una pipeline ETL que cargue los datos de la base de datos Sakila a un data warehouse en BigQuery.

Para ello, será necesario que construyas 3 funciones:
1. get_data_from_sakila: que se encargue de conectarse a la base de datos Sakila y realizar una consulta que
nos permita obtener todas las rentas por cada cliente. Considerar los siguiente:
    - Utilizar las tablas customer, inventory y film. Debes hacer 2 inner joins.
    - De la tabla customer, obtener los campos customer_id, first_name, last_name, email y create_date.
    - De la tabla film, obtener los campos title, description y rating.
    - La tabla inventory solo nos servirá para hacer el vínculo entre customer y film. No se necesitan los campos de esa tabla.
2. transform_data: que se encargue de transformar los datos que acabas de obtener de Sakila.
    - Los datos en la columna first_name y last_name están en mayúsculas. Transformarlos a, primera letra en mayúscula y el resto en minúscula.
    - Extrae la fecha de la columna create_date, eliminando la hora. La fecha debe ir en una nueva columna llamada just_date.
    - Elimina la columna create_date. No la incluyas en el esquema de la tabla de BigQuery.
3. load_data_to_bq: que se encargue de cargar los datos transformados a una tabla de BigQuery.
    - El esquema debe considerar la columna just_date como tipo DATE.
    - La columna customer_id debe ser de tipo INT64.
    - El resto de las columnas deben ser de tipo STRING.

IMPORTANTE: Para revisar la porción del esquema que te ayudará a construir la consulta SQL, puedes consultar la siguiente imagen:
![](./src/sakila.png)
"""