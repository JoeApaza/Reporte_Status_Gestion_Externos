import logging
import os
#import cx_Oracle
import pandas as pd
from dotenv import load_dotenv
from src.database.db_oracle import close_connection_db,read_database_db,leer_sql,get_connection,Insert_dataframe_db,read_database_db
from src.routes.Rutas import ruta_Status_Gestion_General,ruta_Status_Gestion_por_Casas,ruta_env,ruta_html,ruta_libro_Formato
from src.models.Fun_Excel import Macros,Eliminar_Excel,leer_html,enviar_correo,ruta_libro
from datetime import datetime, timedelta
import zipfile

# Obtén la fecha actual
fecha_actual = datetime.now()
fecha_ayer = fecha_actual - timedelta(days=1)
año = fecha_ayer.strftime('%Y')
mes = fecha_ayer.strftime('%m')
dia = fecha_ayer.strftime('%d')


fecha_anteayer = fecha_actual - timedelta(days=2)
año_1 = fecha_anteayer.strftime('%Y')
mes_1 = fecha_anteayer.strftime('%m')
dia_1 = fecha_anteayer.strftime('%d')


logging.basicConfig(format="%(asctime)s::%(levelname)s::%(message)s",   
                    datefmt="%d-%m-%Y %H:%M:%S",    
                    level=10,   
                    filename='.//src//utils//log//app.log',filemode='a')


load_dotenv(ruta_env)
Conexion_Opercom=get_connection(os.getenv('USER_DB'),os.getenv('PASSWORD_DB'),os.getenv('DNS_DB'))
destino_cursor = Conexion_Opercom.cursor()

Df_Status_Gestion_General=pd.read_sql(leer_sql(ruta_Status_Gestion_General), Conexion_Opercom)
    
Macros(ruta_libro_Formato,'Data','A2',Df_Status_Gestion_General,'Reporte_Status_Casas')

Df_Status_Gestion_por_Casas=pd.read_sql(leer_sql(ruta_Status_Gestion_por_Casas), Conexion_Opercom)

gestor =["EMPRESA1","EMPRESA2","EMPRESA3","EMPRESA4","EMPRESA5"]

for i in (gestor):
    df_c = Df_Status_Gestion_por_Casas[Df_Status_Gestion_por_Casas['GESTOR_ASIGNADO'] ==i]
    ruta_archivo = f"./src/models/Reporte_Status_Gestion_{i}.txt"
    df_c.to_csv(ruta_archivo, index=False, quoting=1, sep=',', quotechar='"')


ruta_EMPRESA1 = "./src/models/Reporte_Status_Gestion_EMPRESA1.txt"  # Reemplaza con la ruta y nombre de tu archivo Excel
ruta_EMPRESA2 = "./src/models/Reporte_Status_Gestion_EMPRESA2.txt"  # Reemplaza con la ruta y nombre de tu archivo Excel
ruta_EMPRESA3 = "./src/models/Reporte_Status_Gestion_EMPRESA3.txt"  # Reemplaza con la ruta y nombre de tu archivo Excel
ruta_EMPRESA4 = "./src/models/Reporte_Status_Gestion_EMPRESA4.txt"  # Reemplaza con la ruta y nombre de tu archivo Excel
ruta_EMREPSA5 = "./src/models/Reporte_Status_Gestion_EMPRESA5.txt"  # Reemplaza con la ruta y nombre de tu archivo Excel


 
def comprimir_archivos_txt(ruta_de_salida, archivos_a_comprimir):
    with zipfile.ZipFile(ruta_de_salida, 'w') as zipf:
        for archivo in archivos_a_comprimir:
            try:
                zipf.write(archivo, os.path.basename(archivo), compress_type=zipfile.ZIP_DEFLATED)
            except FileNotFoundError:
                print(f"¡El archivo {archivo} no se encontró!")
 
# Ejemplo de uso:
archivos_a_comprimir = [ruta_EMPRESA1,ruta_EMPRESA2,ruta_EMPRESA3,ruta_EMPRESA4,ruta_EMREPSA5]
ruta_de_salida =  "RUTA_DONDE_PEGARAS\\Detalle_Status_Gestión al "+dia+"."+mes+"."+año+".zip"
 
comprimir_archivos_txt(ruta_de_salida, archivos_a_comprimir)


df_1 = Df_Status_Gestion_General.groupby(['TIPO_CORPORATIVO','GESTOR_ASIGNADO']).agg({'TOTAL_A_GESTIONAR_S': 'sum','POR_VENCER': 'sum','VENCIDA': 'sum','DEUDA_SOLES': 'sum','RECLAMO_SOLES': 'sum','CUENTAS': 'sum'}).reset_index()
df_1['%EFECTIVIDAD'] = (df_1['TOTAL_A_GESTIONAR_S']-df_1['DEUDA_SOLES'])/(df_1['TOTAL_A_GESTIONAR_S']-df_1['POR_VENCER'])*100
df_1['TOTAL_A_GESTIONAR_S']=df_1['TOTAL_A_GESTIONAR_S'].apply(lambda x:'{:,.0f}'.format(x))
df_1['POR_VENCER']=df_1['POR_VENCER'].apply(lambda x:'{:,.0f}'.format(x))
df_1['VENCIDA']=df_1['VENCIDA'].apply(lambda x:'{:,.0f}'.format(x))
df_1['DEUDA_SOLES']=df_1['DEUDA_SOLES'].apply(lambda x:'{:,.0f}'.format(x))
df_1['RECLAMO_SOLES']=df_1['RECLAMO_SOLES'].apply(lambda x:'{:,.0f}'.format(x))
df_1['CUENTAS']=df_1['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_1['%EFECTIVIDAD']=df_1['%EFECTIVIDAD'].apply(lambda x:'{:,.2f}'.format(x))
df_1=df_1.apply(lambda x: x.astype(str).str.capitalize())


df_1_1 = Df_Status_Gestion_General.groupby(['SEGMENTO_COMERCIAL','GESTOR_ASIGNADO']).agg({'TOTAL_A_GESTIONAR_S': 'sum','POR_VENCER': 'sum','VENCIDA': 'sum','DEUDA_SOLES': 'sum','RECLAMO_SOLES': 'sum','CUENTAS': 'sum'}).reset_index()
df_1_1['%EFECTIVIDAD'] = (df_1_1['TOTAL_A_GESTIONAR_S']-df_1_1['DEUDA_SOLES'])/(df_1_1['TOTAL_A_GESTIONAR_S']-df_1_1['POR_VENCER'])*100
df_1_1['TOTAL_A_GESTIONAR_S']=df_1_1['TOTAL_A_GESTIONAR_S'].apply(lambda x:'{:,.0f}'.format(x))
df_1_1['POR_VENCER']=df_1_1['POR_VENCER'].apply(lambda x:'{:,.0f}'.format(x))
df_1_1['VENCIDA']=df_1_1['VENCIDA'].apply(lambda x:'{:,.0f}'.format(x))
df_1_1['DEUDA_SOLES']=df_1_1['DEUDA_SOLES'].apply(lambda x:'{:,.0f}'.format(x))
df_1_1['RECLAMO_SOLES']=df_1_1['RECLAMO_SOLES'].apply(lambda x:'{:,.0f}'.format(x))
df_1_1['CUENTAS']=df_1_1['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_1_1['%EFECTIVIDAD']=df_1_1['%EFECTIVIDAD'].apply(lambda x:'{:,.2f}'.format(x))
df_1_1=df_1_1.apply(lambda x: x.astype(str).str.capitalize())


df_2 = Df_Status_Gestion_General.groupby(['NOMBRE_CARTERA','GESTOR_ASIGNADO']).agg({'TOTAL_A_GESTIONAR_S': 'sum','POR_VENCER': 'sum','VENCIDA': 'sum','DEUDA_SOLES': 'sum','RECLAMO_SOLES': 'sum','CUENTAS': 'sum'}).reset_index()
df_2['%EFECTIVIDAD'] = (df_2['TOTAL_A_GESTIONAR_S']-df_2['DEUDA_SOLES'])/(df_2['TOTAL_A_GESTIONAR_S']-df_2['POR_VENCER'])*100
df_2['TOTAL_A_GESTIONAR_S']=df_2['TOTAL_A_GESTIONAR_S'].apply(lambda x:'{:,.0f}'.format(x))
df_2['POR_VENCER']=df_2['POR_VENCER'].apply(lambda x:'{:,.0f}'.format(x))
df_2['VENCIDA']=df_2['VENCIDA'].apply(lambda x:'{:,.0f}'.format(x))
df_2['DEUDA_SOLES']=df_2['DEUDA_SOLES'].apply(lambda x:'{:,.0f}'.format(x))
df_2['RECLAMO_SOLES']=df_2['RECLAMO_SOLES'].apply(lambda x:'{:,.0f}'.format(x))
df_2['CUENTAS']=df_2['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_2['%EFECTIVIDAD']=df_2['%EFECTIVIDAD'].apply(lambda x:'{:,.2f}'.format(x))
df_2=df_2.apply(lambda x: x.astype(str).str.capitalize())


df_3_p =Df_Status_Gestion_General.loc[(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'CON DEUDA VENCIDA')|(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'SIN DEUDA')]
df_3 = df_3_p.pivot_table(index=['TIPO_CORPORATIVO','GESTOR_ASIGNADO'], columns='RESULTADO', values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
total_columna =  df_3_p.pivot_table(index=['TIPO_CORPORATIVO','GESTOR_ASIGNADO'], values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
df_3_t =pd.merge(df_3, total_columna, on=['TIPO_CORPORATIVO', 'GESTOR_ASIGNADO'], how='outer')
df_3_t['%CONTACTO'] = df_3_t['CONTACTO']/df_3_t['CUENTAS']*100
df_3_t['%NO_CONTACTO'] = df_3_t['NO CONTACTO']/df_3_t['CUENTAS']*100
df_3_t['%SIN_GESTION'] = df_3_t['SIN GESTION']/df_3_t['CUENTAS']*100
df_3_t['CONTACTO']=df_3_t['CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_3_t['NO CONTACTO']=df_3_t['NO CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_3_t['SIN GESTION']=df_3_t['SIN GESTION'].apply(lambda x:'{:,.0f}'.format(x))
df_3_t['CUENTAS']=df_3_t['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_3_t['%CONTACTO']=df_3_t['%CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_3_t['%NO_CONTACTO']=df_3_t['%NO_CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_3_t['%SIN_GESTION']=df_3_t['%SIN_GESTION'].apply(lambda x:'{:,.2f}%'.format(x))
df_3_t=df_3_t.apply(lambda x: x.astype(str).str.capitalize())


df_3_3_p = Df_Status_Gestion_General.loc[(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'CON DEUDA VENCIDA')|(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'SIN DEUDA')]
df_3_3 = df_3_3_p.pivot_table(index=['SEGMENTO_COMERCIAL','GESTOR_ASIGNADO'], columns='RESULTADO', values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
total_columna =  df_3_3_p.pivot_table(index=['SEGMENTO_COMERCIAL','GESTOR_ASIGNADO'], values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
df_3_3_t =pd.merge(df_3_3, total_columna, on=['SEGMENTO_COMERCIAL', 'GESTOR_ASIGNADO'], how='outer')
df_3_3_t['%CONTACTO'] = df_3_3_t['CONTACTO']/df_3_3_t['CUENTAS']*100
df_3_3_t['%NO_CONTACTO'] = df_3_3_t['NO CONTACTO']/df_3_3_t['CUENTAS']*100
df_3_3_t['%SIN_GESTION'] = df_3_3_t['SIN GESTION']/df_3_3_t['CUENTAS']*100
df_3_3_t['CONTACTO']=df_3_3_t['CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_3_3_t['NO CONTACTO']=df_3_3_t['NO CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_3_3_t['SIN GESTION']=df_3_3_t['SIN GESTION'].apply(lambda x:'{:,.0f}'.format(x))
df_3_3_t['CUENTAS']=df_3_3_t['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_3_3_t['%CONTACTO']=df_3_3_t['%CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_3_3_t['%NO_CONTACTO']=df_3_3_t['%NO_CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_3_3_t['%SIN_GESTION']=df_3_3_t['%SIN_GESTION'].apply(lambda x:'{:,.2f}%'.format(x))
df_3_3_t=df_3_3_t.apply(lambda x: x.astype(str).str.capitalize())


df_4_p = Df_Status_Gestion_General.loc[(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'CON DEUDA VENCIDA')|(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'SIN DEUDA')]
df_4 = df_4_p.pivot_table(index=['NOMBRE_CARTERA','GESTOR_ASIGNADO'], columns='RESULTADO', values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
total_columna =  df_4_p.pivot_table(index=['NOMBRE_CARTERA','GESTOR_ASIGNADO'], values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
df_4_t =pd.merge(df_4, total_columna, on=['NOMBRE_CARTERA', 'GESTOR_ASIGNADO'], how='outer')
df_4_t['%CONTACTO'] = df_4_t['CONTACTO']/df_4_t['CUENTAS']*100
df_4_t['%NO_CONTACTO'] = df_4_t['NO CONTACTO']/df_4_t['CUENTAS']*100
df_4_t['%SIN_GESTION'] = df_4_t['SIN GESTION']/df_4_t['CUENTAS']*100
df_4_t['CONTACTO']=df_4_t['CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_4_t['NO CONTACTO']=df_4_t['NO CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_4_t['SIN GESTION']=df_4_t['SIN GESTION'].apply(lambda x:'{:,.0f}'.format(x))
df_4_t['CUENTAS']=df_4_t['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_4_t['%CONTACTO']=df_4_t['%CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_4_t['%NO_CONTACTO']=df_4_t['%NO_CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_4_t['%SIN_GESTION']=df_4_t['%SIN_GESTION'].apply(lambda x:'{:,.2f}%'.format(x))
df_4_t=df_4_t.apply(lambda x: x.astype(str).str.capitalize())


df_5_p =Df_Status_Gestion_General.loc[(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'CON DEUDA VENCIDA')|(Df_Status_Gestion_General['OBS_VENCIMIENTO'] == 'SIN DEUDA')]
df_5 = df_5_p.pivot_table(index=['REGION','GESTOR_ASIGNADO'], columns='RESULTADO', values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
total_columna =  df_5_p.pivot_table(index=['GESTOR_ASIGNADO','REGION'], values='CUENTAS', aggfunc='sum').reset_index().fillna(0)
df_5_t =pd.merge(df_5, total_columna, on=['GESTOR_ASIGNADO', 'REGION'], how='outer')
df_5_t['%CONTACTO'] = df_5_t['CONTACTO']/df_5_t['CUENTAS']*100
df_5_t['%NO_CONTACTO'] = df_5_t['NO CONTACTO']/df_5_t['CUENTAS']*100
df_5_t['%SIN_GESTION'] = df_5_t['SIN GESTION']/df_5_t['CUENTAS']*100
df_5_t['CONTACTO']=df_5_t['CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_5_t['NO CONTACTO']=df_5_t['NO CONTACTO'].apply(lambda x:'{:,.0f}'.format(x))
df_5_t['SIN GESTION']=df_5_t['SIN GESTION'].apply(lambda x:'{:,.0f}'.format(x))
df_5_t['CUENTAS']=df_5_t['CUENTAS'].apply(lambda x:'{:,.0f}'.format(x))
df_5_t['%CONTACTO']=df_5_t['%CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_5_t['%NO_CONTACTO']=df_5_t['%NO_CONTACTO'].apply(lambda x:'{:,.2f}%'.format(x))
df_5_t['%SIN_GESTION']=df_5_t['%SIN_GESTION'].apply(lambda x:'{:,.2f}%'.format(x))
df_5_t=df_5_t.apply(lambda x: x.astype(str).str.capitalize())

ruta_libro = "./src/models/Reporte Status Gestión Casas de Cobranza al "+dia+"."+mes+"."+año+".xlsx"  # Reemplaza con la ruta y nombre de tu archivo Excel

html=leer_html(ruta_html,df_1,df_1_1,df_2,df_3_t,df_3_3_t,df_4_t,df_5_t)

Name_File_1 = "Reporte Status Gestión Casas de Cobranza al "+dia_1+"."+mes_1+"."+año_1+""  # Reemplaza con la ruta y nombre de tu archivo Excel


Eliminar_Excel(f'{Name_File_1}.html')
Eliminar_Excel(f'{Name_File_1}.pdf')

Name_File = "Reporte Status Gestión Casas de Cobranza al "+dia+"."+mes+"."+año+""  # Reemplaza con la ruta y nombre de tu archivo Excel

with open(f'./{Name_File}.html', 'w') as f:
    f.write(html)

import pdfkit
 
path_to='D:\\wkhtmltopdf\\bin\\wkhtmltopdf.exe'
path_file = f'{Name_File}.html'
config=pdfkit.configuration(wkhtmltopdf=path_to)
 
pdf_options = {
    'page-size': 'Letter',
    'margin-top': '0in',
    'margin-right': '0in',
    'margin-bottom': '0in',
    'margin-left': '0in'
}

pdfkit.from_file(path_file,output_path=f'./{Name_File}.pdf',options=pdf_options,configuration=config)


enviar_correo(html,ruta_libro)
#enviar_correo(html)
Eliminar_Excel(ruta_libro)
destino_cursor.close
close_connection_db(Conexion_Opercom)
