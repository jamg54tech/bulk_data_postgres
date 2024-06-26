import psycopg2
import dask.dataframe as dd
from datetime import datetime
import time
import os
from psycopg2 import sql;

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
        cur.copy_from(f, 'liverpool_sku_promotion', sep='|')
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
        cur.copy_from(f, 'lp_institutional_promotion', sep='|')
    # Commit the transaction
    conn.commit()


def process_relations_file(conn,cur,base_path:str,file_name:str):
    filepath=base_path+file_name
    assert os.path.isfile(filepath)

    print("PATH:", filepath);
    with open(filepath, 'r') as csv_buffer:
        #next(csv_buffer) #skip header
        cur.copy_from(csv_buffer, 'lp_instnl_sku_promotion', sep='|')
    csv_buffer.close();
    # Commit the transaction
    conn.commit()

def main():

    print("INICIANDO PROCESO DE CARGA DE DATOS:",datetime.now());
    print("\n CREANDO CONEXION...");

    conn = psycopg2.connect(
        dbname="promotions",
        user = "postgres",
        password = "admin123",
        host = "localhost",
        port = "5432"
    )

    #Folder where promotions files will be stored
    base_path="C:\\Users\\jamolinag\\Desktop\\liverpool\\liverpool_promoitons_files\\23MAYO2023\\";
    # Create a cursor object using the connection
    cur = conn.cursor()

    try:

        print("\n\n==============PROCESANDO ARCHIVO DE PROMOCIONES...");

        print("BORRANDO DATOS DE TABLA liverpool_sku_promotion:")
        query = sql.SQL("TRUNCATE TABLE liverpool_sku_promotion CASCADE;")
        cur.execute(query)
        conn.commit()


        print("BORRANDO CONSTRAINT liverpool_sku_promotion_pkey")
        query = sql.SQL("ALTER TABLE liverpool_sku_promotion DROP CONSTRAINT IF EXISTS liverpool_sku_promotion_pkey CASCADE;")
        cur.execute(query)
        conn.commit()


        # Define the COPY command for promotions
        print("INSERTANDO DATOS EN LA TABLA promotions");
        promotions_file = "liverpool_sku_promotion.txt";
        process_sku_promotion_file(conn,cur,base_path,promotions_file);

        print("CREANDO CONSTRAINT liverpool_sku_promotion_pkey:")
        query = sql.SQL("ALTER TABLE liverpool_sku_promotion ADD PRIMARY KEY (promo_id);")
        cur.execute(query)
        conn.commit()

        print("\n\n==============PROCESANDO ARCHIVO DE SKUS...");

        print("BORRANDO DATOS DE TABLA lp_institutional_promotion:")
        query = sql.SQL("TRUNCATE TABLE lp_institutional_promotion CASCADE;")
        cur.execute(query)
        conn.commit()

        print("BORRANDO CONSTRAINT lp_institutional_promotion_pkey")
        query = sql.SQL("ALTER TABLE lp_institutional_promotion DROP CONSTRAINT IF EXISTS lp_institutional_promotion_pkey CASCADE;")
        cur.execute(query)
        conn.commit()

        print("INSERTANDO DATOS EN LA TABLA lp_institutional_promotion")
        lp_inst_sku_promotion_file = "lp_institutional_promotion.txt"
        process_lp_institution_sku_promotion_file(conn,cur,base_path,lp_inst_sku_promotion_file)

        print("CREANDO CONSTRAINT lp_institutional_promotion_pkey:")
        query = sql.SQL("ALTER TABLE lp_institutional_promotion ADD PRIMARY KEY (institutional_promo_id);")
        cur.execute(query)
        conn.commit()

        print("\n\n==============PROCESANDO ARCHIVO DE RELACIONES...");

        print("BORRANDO CONSTRAINT inst_promo_id_foreign_key:")
        query = sql.SQL("ALTER TABLE lp_instnl_sku_promotion DROP CONSTRAINT IF EXISTS inst_promo_id_foreign_key;")
        cur.execute(query)
        conn.commit()

        print("BORRANDO CONSTRAINT promo_id_foreign_key:")
        query = sql.SQL("ALTER TABLE lp_instnl_sku_promotion DROP CONSTRAINT IF EXISTS promo_id_foreign_key;")
        cur.execute(query)
        conn.commit()

        print("BORRANDO DATOS DE TABLA lp_instnl_sku_promotion:")
        query = sql.SQL("TRUNCATE TABLE lp_instnl_sku_promotion;")
        cur.execute(query)
        conn.commit()

        print("REGISTRANDO ROWS")
        lp_relations_file = "lp_instnl_sku_promotion.txt"
        process_relations_file(conn,cur,base_path,lp_relations_file);

        print("CREANDO CONSTRAINT inst_promo_id_foreign_key:")
        query = sql.SQL("ALTER TABLE lp_instnl_sku_promotion ADD CONSTRAINT inst_promo_id_foreign_key FOREIGN KEY (institutional_promo_id) REFERENCES lp_institutional_promotion (institutional_promo_id) ON DELETE CASCADE ON UPDATE CASCADE;")
        cur.execute(query)
        conn.commit()

        print("CREANDO CONSTRAINT promo_id_foreign_key:")
        query = sql.SQL("ALTER TABLE lp_instnl_sku_promotion ADD CONSTRAINT promo_id_foreign_key FOREIGN KEY (sku_promotions) REFERENCES liverpool_sku_promotion (promo_id) ON DELETE CASCADE ON UPDATE CASCADE;")
        cur.execute(query)
        conn.commit()


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