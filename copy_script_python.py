import psycopg2
import dask.dataframe as dd
from datetime import datetime
import time
import os


def process_sku_promotion_file(conn,cur,base_path:str,file_name:str):

    filepath=base_path+file_name
    print("PATH:", filepath);
    #reading csv to use it as dask dataframe
    df_promotions = dd.read_csv(filepath, sep='|', header=None,dtype=str);
    # Drop the last two columns
    # Determine the total number of columns
    num_columns = len(df_promotions.columns)
    # Specify the last two columns to be deleted
    columns_to_delete = [num_columns - 2, num_columns - 1]
    # Drop the last two columns from the DataFrame
    df_promotions = df_promotions.drop(columns_to_delete, axis=1)

    #wrting temp file to use it inside copy command on table promotions
    temp_file = base_path + "tempfile1.csv";
    df_promotions.to_csv(temp_file, index=False, single_file=True, header=False, sep="|");
    assert os.path.isfile(temp_file)
    with open(temp_file, 'r') as f:
        #next(csv_buffer) #skip header
        cur.copy_from(f, 'promotions', sep='|')
    # Commit the transaction
    conn.commit()

def process_lp_institution_sku_promotion_file(conn,cur,base_path:str,file_name:str):

    filepath=base_path+file_name
    print("PATH:", filepath);
    #reading csv to use it as dask dataframe
    df_promotions = dd.read_csv(filepath, sep='|', header=None,dtype=str)

    # Generate a timestamp
    timestamp = datetime.now();

    column_index = 1  # Assuming you want to replace the values in the second column

    # Rename the column temporarily
    df_temp = df_promotions.rename(columns={column_index: 'temp_column'})

    # Assign the constant value to the column
    df_temp = df_temp.assign(temp_column=timestamp)

    # Rename the column back to its original name
    df_temp = df_temp.rename(columns={'temp_column': column_index})

    temp_file=base_path+"tempfile2.csv";

    df_temp.to_csv(temp_file, index=False, single_file=True, header=False,sep="|");

    assert os.path.isfile(temp_file)

    with open(temp_file, 'r') as f:
        #next(csv_buffer) #skip header
        cur.copy_from(f, 'skus', sep='|')
    # Commit the transaction
    conn.commit()


def process_relations_file(conn,cur,base_path:str,file_name:str):
    filepath=base_path+file_name
    assert os.path.isfile(filepath)

    print("PATH:", filepath);
    with open(filepath, 'r') as csv_buffer:
        #next(csv_buffer) #skip header
        cur.copy_from(csv_buffer, 'sku_promo_relations', sep='|')
    # Commit the transaction
    conn.commit()

def main():

    print("INICIANDO PROCESO DE CARGA DE DATOS:",datetime.now());
    print("\n CREANDO CONEXION...");

    conn = psycopg2.connect(
        dbname="promotions",
        user = "postgres",
        password = "n0m3l0",
        host = "localhost",
        port = "5432"
    )

    #Folder where promotions files will be stored
    base_path="C:\\Users\\JAMG54\\Desktop\\LIVERPOOL\\POCS\\PostgresCOPY\\PROMOTIONS\\";
    # Create a cursor object using the connection
    cur = conn.cursor()

    try:

        print("\n\n==============PROCESANDO ARCHIVO DE PROMOCIONES...");
        # Define the COPY command for promotions
        promotions_file = "liverpool_sku_promotion.txt.07032024";
        process_sku_promotion_file(conn,cur,base_path,promotions_file);

        print("\n\n==============PROCESANDO ARCHIVO DE SKUS...");

        lp_inst_sku_promotion_file = "lp_institutional_promotion.txt.07032024"
        process_lp_institution_sku_promotion_file(conn,cur,base_path,lp_inst_sku_promotion_file)

        print("\n\n==============PROCESANDO ARCHIVO DE RELACIONES...");
        lp_relations_file = "lp_instnl_sku_promotion.txt.07032024"
        process_relations_file(conn,cur,base_path,lp_relations_file);

        print("\n\n PROCESAMIENTO EXITOSO")

    except Exception as e:
        print("ERROR:",str(e));

    # Close the cursor and connection
    cur.close()
    conn.close()

    print("\n\n PROCESO FINALIZADO:",datetime.now());


start_time = time.time()

main();

end_time = time.time()

execution_time = end_time - start_time
print("Tiempo de ejecución:", execution_time, "segundos")
