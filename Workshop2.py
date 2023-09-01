import mysql.connector
import pandas as pd

datos = pd.read_csv('C:\ETLworkshop\candidates.csv', sep =";")
datos
config = {
    'user': 'root',
    'password': 'Octubre03',
    'host': 'localhost',  
    'database': 'candidates',
}

try:
    conn = mysql.connector.connect(**config)
    if conn.is_connected():
        print("Conexión exitosa a la base de datos MySQL")


except mysql.connector.Error as e:
    print(f"Error al conectar a la base de datos: {e}")

finally:
    conn.close()



create_table_query = """
CREATE TABLE IF NOT EXISTS ContratosAplicantes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    application_date DATE,
    country VARCHAR(250),
    yoe INT,
    seniority VARCHAR(250),
    technology VARCHAR(100),
    code_challenge_score FLOAT,
    technical_interview_score FLOAT,
    is_hired BOOLEAN
);
"""

insert_query = """
INSERT INTO ContratosAplicantes (first_name, last_name, email, application_date, country, yoe, seniority, technology, code_challenge_score, technical_interview_score, is_hired)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

try:
    cnx = mysql.connector.connect(**config)

    if cnx.is_connected():
        cursor = cnx.cursor()

        cursor.execute(create_table_query)

        for index, row in datos.iterrows():
            code_challenge_score = row['Code Challenge Score']
            technical_interview_score = row['Technical Interview Score']
            is_hired = code_challenge_score >= 7 and technical_interview_score >= 7

            values = (
                row['First Name'], row['Last Name'], row['Email'], row['Application Date'], row['Country'],
                row['YOE'], row['Seniority'], row['Technology'], code_challenge_score, technical_interview_score, is_hired
            )
            cursor.execute(insert_query, values)

        cnx.commit()

        print("Datos insertados exitosamente en la tabla ContractedApplicants.")

except mysql.connector.Error as e:
    print("Error al ejecutar la consulta:", e)

finally:
    if cnx:
        cnx.close()
    
