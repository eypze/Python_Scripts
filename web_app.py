# web_app.py

# Web page for select tables and fields to create in cloud
def web_app():
    import streamlit as st
    import pandas as pd

    csv_file = r"D:\Users\eypze\Documents\Project Very Fast Data\tables_hana.csv"

    # 1. read CSV file
    df = pd.read_csv(csv_file, sep=";")

    st.title("BD Schema Explorer")

    # Dictionary for save selection
    seleccion = []

    # Callback for "Select all" to update all fields
    def update_all_fields(all_key, field_keys):
        # get the current value of the "Select All" checkbox
        is_selected = st.session_state[all_key]
        for f_key in field_keys:
            st.session_state[f_key] = is_selected

    # 2. Create hierarchy: DB -> Tables -> Fields
    for db in df['DB_NAME'].unique():
        st.header(f"📂 {db}")
        db_tables = df[df['DB_NAME'] == db]['TABNAME'].unique()
    
        for table in db_tables:
            with st.expander(f"📑 Table: {table}", expanded=False):
                table_fields = df[(df['DB_NAME'] == db) & (df['TABNAME'] == table)]
                
                # Collect all field keys for this table
                current_field_keys = [f"{db}_{table}_{row['FIELDNAME']}" for _, row in table_fields.iterrows()]
                all_key = f"ALL_{db}_{table}"

                # Checkbox for select all table
                st.checkbox(f"Select all fields : {table}", key=all_key, 
                            on_change=update_all_fields, args=(all_key, current_field_keys))
            
                for _, row in table_fields.iterrows():
                    decimals = 0
                    try:
                        decimals = int(row['DECIMALS'])
                    except (ValueError, TypeError):
                        pass

                    if decimals > 0:
                        field_label = f"{row['DDTEXT']} ({row['FIELDNAME']}) [{row['DATATYPE']} {row['LENG']}, {decimals}]"
                    else:
                        field_label = f"{row['DDTEXT']} ({row['FIELDNAME']}) [{row['DATATYPE']} {row['LENG']}]"
                    
                    checkbox_key = f"{db}_{table}_{row['FIELDNAME']}"

                    # Initialize session state for this key if not present (default to False)
                    if checkbox_key not in st.session_state:
                         st.session_state[checkbox_key] = False

                    if st.checkbox(field_label, key=checkbox_key):
                        seleccion.append(row)

    # 3. Show selection
    if seleccion:
        seleccion_df = pd.DataFrame(seleccion)
        st.subheader("✅ Selected fields")
        st.dataframe(seleccion_df)

    # 4. Download CSV button 
        csv = seleccion_df.to_csv(index=False, sep=";").encode("utf-8") 
        st.download_button( 
            label="⬇️ Download selection in CSV", 
            data=csv, file_name="selection.csv", 
            mime="text/csv" )

def main():
     web_app() # execute with "streamlit run web_app.py" on terminal

if __name__ == '__main__':
  main()