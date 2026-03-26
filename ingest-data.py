import pandas as pd
import sqlalchemy 
import math
from tqdm.auto import tqdm

def main():

    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

    print('iniciando la descarga')

    datos_crudos = pd.read_parquet(URL)

    print('Datos descargados del internet (NY Taxy GOV)')
    print(datos_crudos.info())
    print(f'Cantidad de datos: {datos_crudos.shape[0]}')
    print(f'Columnas: {datos_crudos.columns}')
    print(datos_crudos.head())
    print()

    
    conexion = sqlalchemy.create_engine('postgresql://root:root@hola-mundo-datos-data-warehouse-1:5432/warehouse')


    # Agregar las filas por chunks (grupos)
    # round -> .5 -> 1
    # ceil() -> 0.0000001 -> 1
    # floor()  -> .99999 -> 0
    
    #for i in range(1, math.ceil(datos_crudos.shape[0]/tamano)):

    # chunking segmentar los datos en grupos de 10000

    tamano = 10000

    num_chunks = math.ceil(datos_crudos.shape[0]/tamano)
    inicio = 0
    fin = tamano

    # no importa cuando ejecute el script siempre da el mismo resultado

    print('Creación de la table')
    datos_crudos.head(0).to_sql(
        name='viajes_taxi_amarillo',
        con=conexion,
        if_exists='replace'
    )

    print('Inicio de guardado en el almacen de datos en el warehouse')

    for i in tqdm(range(1, num_chunks)):
        # indexacion - slice [incluyente:excluyente]
        # 0 : 10000
        # 10000 : 20000

        datos_crudos.iloc[inicio:fin].to_sql(
            name ='viajes_taxi_amarillo',
            con = conexion, 
            if_exists='append'
        )

        inicio = fin
        fin = tamano * i

    print('Se guardaron los datos exitosamente en el warehouse')

#verificamos que el archivo se ejecuta como principal
if __name__ == '__main__':
    main()
