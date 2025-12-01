from flask import Flask, render_template, request
import mysql.connector
import pandas as pd

app = Flask(__name__)

# Configuración CORRECTA (sin socket, con usuario ProyectoDB)
db_config = {
    'host': 'localhost',
    'user': 'ProyectoDB',
    'password': 'PDB',
    'database': 'music_analysis'
}

def get_tables():
    """Obtener lista de tablas en la base de datos"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        conn.close()
        print(f"DEBUG: Conexión exitosa. Tablas: {tables}")
        return tables
    except Exception as e:
        print(f"DEBUG: Error en get_tables(): {e}")
        return []

def get_table_data(table_name):
    """Obtener datos de una tabla específica"""
    try:
        conn = mysql.connector.connect(**db_config)
        query = f"SELECT * FROM {table_name} LIMIT 100"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"DEBUG: Error en get_table_data({table_name}): {e}")
        return pd.DataFrame()

@app.route('/')
def index():
    tables = get_tables()
    selected_table = request.args.get('table', tables[0] if tables else '')
    
    data = None
    if selected_table:
        data = get_table_data(selected_table)
    
    print(f"DEBUG: Tablas encontradas: {tables}")
    print(f"DEBUG: Tabla seleccionada: {selected_table}")
    if data is not None:
        print(f"DEBUG: Datos obtenidos: {len(data)} filas")
    else:
        print(f"DEBUG: No se obtuvieron datos")
    
    return render_template('index.html', 
                         tables=tables, 
                         selected_table=selected_table, 
                         data=data)

if __name__ == '__main__':
    print("=== INICIANDO APLICACIÓN FLASK ===")
    print("DEBUG: Verificando conexión a MySQL...")
    
    try:
        test_tables = get_tables()
        print(f"DEBUG: Tablas en MySQL: {test_tables}")
    except Exception as e:
        print(f"DEBUG: Error conectando a MySQL: {e}")
    
    print("DEBUG: Servidor iniciando en http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
