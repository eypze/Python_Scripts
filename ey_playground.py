# CSV file mangement

import pandas as pd

def file_csv():
    tfile = r"C:\Users\eypze\open-sap-python\BHP.csv"
    f = open(tfile)
    for line in f:
        words=line.split(';')
        print(words)
        for word in words:  
            print(word)
    f.close()

    # 1. Leer el archivo CSV
    df = pd.read_csv(tfile, sep=";")
    print(df)    

# TXT file management
def file_txt():
    tfile = r"C:\Users\eypze\open-sap-python\google-python-exercises\google-python-exercises\basic\small.txt"
    f = open(tfile, encoding="utf-8")
    for line in f:
        print(line, end='')
    f.close()

# Insert data into BD table
def insert_data():
    import pyodbc
    import csv

    db_file = r"C:\Users\eypze\open-sap-python\DB\Database1.accdb"
    conn = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={db_file};"  # r=raw string. f=allow variable in string
        )
    cursor = conn.cursor()
    
    # execute sql query 
    cursor.execute("SELECT * FROM Table1") 
    # get column names 
    column_names = [desc[0] for desc in cursor.description] 
    del column_names[0]
    columns_db = ",".join(column_names) #convert list to string
    values_db = ",".join(["?"] * len(column_names))

    csv_file = r"C:\Users\eypze\open-sap-python\BHP.csv"

    with open(csv_file, newline='') as f:
        reader = csv.reader(f, delimiter=";")
        next(reader) # header skip

        data = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in reader]
        #sql_query = f"INSERT INTO Table1 (id_type, sales_level, m_c, query, view, dso) VALUES (?, ?, ?, ?, ?, ?)"
        sql_query = f"INSERT INTO Table1 ({columns_db}) VALUES ({values_db})"
        sql_query_del = r"DELETE FROM Table1"
      
        cursor.execute(sql_query_del)
        cursor.executemany(sql_query, data)

    conn.commit()

    print(f"Se insertaron {len(data)} registros en la tabla Clientes.")

    conn.close()

# Create tables
def create_tables():
    import pyodbc
    import csv
    from collections import Counter

    csv_file = r"C:\Users\eypze\open-sap-python\tables.csv"

    db_file = r"C:\Users\eypze\open-sap-python\DB\Database1.accdb"
    conn = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={db_file};"  # r=raw string. f=allow variable in string
        )
    cursor = conn.cursor()

    with open(csv_file, newline='') as f:
        reader = list(csv.DictReader(f, delimiter=";"))
        first_line = [list(row.values())[0] for row in reader]
        tables = Counter(first_line)
        ct = 0
        field = 0
        field_list = str()

        for row in reader:
            field = field + 1
            lines = list(row.values())
            field_type = list(row.values())[2]
            if field < list(tables.values())[ct]:
                if field_type in ('char','text'):
                    fields = lines[1] + ' ' + 'TEXT' + '(' + lines[3] + ')' + ','
                elif field_type in ('dec','decimal'):
                    fields = lines[1] + ' ' + 'DOUBLE' + ',' # + '(' + lines[3] + ',' + lines[4] + ')' + ','
                elif field_type in ('boolean'):
                    fields = lines[1] + ' ' + 'YESNO' + ','
                else: fields = lines[1] + ' ' + field_type.upper + ','
                field_list = field_list + ' ' + fields
                #print(field_list)
            else: 
                if field_type in ('char','text'):
                    fields = lines[1] + ' ' + 'TEXT' + '(' + lines[3] + ')'
                elif field_type in ('dec','decimal'):
                    fields = lines[1] + ' ' + 'DOUBLE' #+ '(' + lines[3] + ',' + lines[4] + ')'
                elif field_type in ('boolean'):
                    fields = lines[1] + ' ' + 'YESNO'
                else: fields = lines[1] + ' ' + field_type.upper()
                field_list = field_list + ' ' + fields
                print(field_list)    

                sql_create = fr"CREATE TABLE {list(tables.keys())[ct]} ({field_list})"
                print(sql_create)
                
                try:
                    cursor.execute(sql_create)
                    conn.commit()
                    print("✅ La tabla fue creada con éxito.") 
                
                except pyodbc.Error as e: print("❌ Error al crear la tabla:", e) 
              
                ct = ct + 1
                field = 0
                field_list = str()

    conn.close() 

# Web page for select tables and fields to create in cloud
def web_app():
    import streamlit as st
    import pandas as pd

    csv_file = r"C:\Users\eypze\open-sap-python\tables2.csv"

    # 1. Cargar archivo CSV
    df = pd.read_csv(csv_file, sep=";")

    st.title("Explorador de Esquema de BD")

    # Diccionario para guardar selección
    seleccion = []

    # 2. Recorrer jerarquía: BD -> Tablas -> Campos
    for db in df['db_name'].unique():
        st.header(f"📂 {db}")
        db_tables = df[df['db_name'] == db]['table_name'].unique()
    
        for table in db_tables:
            with st.expander(f"📑 Tabla: {table}", expanded=False):
                table_fields = df[(df['db_name'] == db) & (df['table_name'] == table)]
            
                # Checkbox para seleccionar toda la tabla
                select_table = st.checkbox(f"Seleccionar toda la tabla {table}", key=f"{db}_{table}")
            
                for _, row in table_fields.iterrows():
                    if select_table or st.checkbox(
                        f"{row['field_name']} ({row['field_type']} {row['field_size']},{row['field_dec']})",
                        key=f"{db}_{table}_{row['field_name']}"
                    ):
                        seleccion.append(row)

    # 3. Mostrar resultados seleccionados
    if seleccion:
        seleccion_df = pd.DataFrame(seleccion)
        st.subheader("✅ Campos seleccionados")
        st.dataframe(seleccion_df)

    # 4. Botón para descargar CSV 
        csv = seleccion_df.to_csv(index=False).encode("utf-8") 
        st.download_button( 
            label="⬇️ Descargar selección en CSV", 
            data=csv, file_name="seleccion.csv", 
            mime="text/csv" )

def hana_odata():
    import requests
    from requests.auth import HTTPBasicAuth
    import xml.etree.ElementTree as ET

    # 1. Configuración de la conexión
    # Reemplaza con tu host, puerto y ruta del servicio
    base_url = "http://s4h2023.sapdemo.com:8003/sap/opu/odata/sap/ZVFD_CDS_VIEW_MARA_CDS"
    entity_set = "/ZVFD_CDS_VIEW_MARA" # La tabla o entidad que quieres consultar
    url = f"{base_url}{entity_set}"
    
    # Credenciales
    username = "S23a85"
    password = "Welcome@1234"

    # 2. Parámetros OData (Filtros, Formato, etc.)
    params = {
        "$format": "xml",    # Cambiamos a XML
        "$top": 10,          # Traer solo los primeros 10 registros (opcional)
    }

    try:
        # 3. Realizar la petición GET
        response = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            params=params
        )

        # 4. Validar respuesta
        if response.status_code == 200:
            print("✅ Conexión exitosa (XML)")
            
            # Parsear respuesta XML (Atom Pub format)
            try:
                root = ET.fromstring(response.content)
                
                # Namespaces comunes en OData v2
                namespaces = {
                    'atom': 'http://www.w3.org/2005/Atom',
                    'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
                    'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
                }
                
                # Buscar todas las entradas <entry>
                entries = root.findall('.//atom:entry', namespaces)
                
                if entries:
                    print(f"Se encontraron {len(entries)} registros.")
                    
                    # Mostrar el primer registro como ejemplo
                    first_entry = entries[0]
                    properties = first_entry.find('.//m:properties', namespaces)
                    
                    if properties is not None:
                        print("Primer registro:")
                        for prop in properties:
                            # Limpiar el nombre del tag para mostrarlo (quitar namespace)
                            tag_name = prop.tag.split('}')[-1] 
                            print(f"  {tag_name}: {prop.text}")
                    else:
                        print("No se encontraron propiedades en el primer registro.")
                        
                else:
                    print("La consulta no devolvió resultados.")

            except ET.ParseError as e:
                print(f"❌ Error al parsear XML: {e}")
                print("Contenido recibido:", response.text[:500]) # Mostrar inicio para debug
        
        else:
            print(f"❌ Error {response.status_code}: {response.text}")

    except Exception as e:
        print(f"❌ Error de conexión: {str(e)}")
    

def main():
    # file_csv()
    # file_txt()
    # insert_data()
    # create_tables()
    # web_app() # execute with "streamlit run sample_scripts.py" on terminal
     hana_odata()

if __name__ == '__main__':
  main()