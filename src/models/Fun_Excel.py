
import logging
import xlwings as xw
import os
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from jinja2 import Template
import win32com.client as win32


fecha_actual = datetime.now()
fecha_ayer = fecha_actual - timedelta(days=1)
año = fecha_ayer.strftime('%Y')
mes = fecha_ayer.strftime('%m')
dia = fecha_ayer.strftime('%d')

def Macros(ruta_libro_formato,hoja,rango_inicio,dataframe,Nombre_Macro):
        logging.info('Iniciando proceso para ejecucion de macro')
        app = xw.App(visible=False)
        logging.info('Ejecutando macro sin hacer visible que se abra excel')
        wb = xw.Book(ruta_libro_formato)
        logging.info('Se abrio archivo excel')
        sheet = wb.sheets[hoja]
        sheet.range(rango_inicio).value = dataframe.values
        try:
            wb.macro(Nombre_Macro).run()
            logging.info(f"La macro '{Nombre_Macro}' se ha ejecutado con éxito.")
        except Exception as e:
            logging.info(f"Error al ejecutar la macro: {e}")
        wb.close()
        logging.info('Se cerro archivo excel')
        app.quit()
        logging.info('hacer visible que se abra excel')
     

def Eliminar_Excel(ruta_libro):
    if os.path.exists(ruta_libro):
        os.remove(ruta_libro)
        logging.info(f"El archivo {ruta_libro} se ha eliminado con éxito.")
    else:
        logging.info(f"El archivo {ruta_libro} no existe.")

     

def leer_html(ruta_html,dataframe,dataframe1,dataframe2,dataframe3,dataframe3_3,dataframe4,dataframe5):
    ruta_html=Path(ruta_html)
    with open(ruta_html,'r',encoding='utf-8') as file:
         template_html=file.read()
         template=Template(template_html)
         return template.render(columns=dataframe.columns,data=dataframe,columns1=dataframe1.columns,data1=dataframe1,columns2=dataframe2.columns,data2=dataframe2,columns3=dataframe3.columns,data3=dataframe3,columns3_3=dataframe3_3.columns,data3_3=dataframe3_3,columns4=dataframe4.columns,data4=dataframe4,columns5=dataframe5.columns,data5=dataframe5)


def enviar_correo(html,ruta_libro):
    outlook= win32.Dispatch('outlook.application')
    mail=outlook.createitem(0)
    mail.subject="ASUNTO"
    attachment = os.path.abspath(ruta_libro)
    mail.Attachments.Add(attachment)
    mail.to='CORREOS'
    mail.CC='COPIAS'
    mail.HTMLBody=html
    mail.GetInspector 
    mail.Send()
    logging.info(f"ENVIADO" )


