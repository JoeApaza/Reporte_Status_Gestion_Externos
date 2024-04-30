SELECT X.TIPO_CORPORATIVO,X.SEGMENTO_COMERCIAL,
SUM(X.TOTAL_A_GESTIONAR_S)TOTAL_A_GESTIONAR_S,
SUM(X.POR_VENCER)POR_VENCER,
SUM(X.VENCIDA)VENCIDA,
SUM(X.DEUDA_SOLES)DEUDA_SOLES,
SUM(X.RECLAMO_SOLES)RECLAMO_SOLES,
COALESCE(X.OBS_VENCIMIENTO,'SIN DEUDA')OBS_VENCIMIENTO,
(CASE WHEN SUM(X.DEUDA_SOLES)=0 THEN 'SIN DEUDA' ELSE 'CON DEUDA' END)OBS_DEUDA,
(CASE WHEN SUM(X.RECLAMO_SOLES)=0 THEN 'SIN RECLAMO' ELSE 'CON RECLAMO' END)OBS_RECLAMO,
COUNT(DISTINCT X.CUST_ACCOUNT)CUENTAS,
X.DEPARTAMENTO,
X.GESTOR_ASIGNADO,
X.NOMBRE_CARTERA,
X.REGION,
X.TIPO_GESTION,
X.TIPIFICACION,
X.ESCENARIO_TIPIFICACION,
X.RESULTADO
FROM
(SELECT DISTINCT C.CUST_ACCOUNT,C.RUC_DNI, C.NOMBRE_COMPLETO_CLIENTE,
C.TIPO_CORPORATIVO,C.SEGMENTO_COMERCIAL,
C.TOTAL_A_GESTIONAR_S,C.POR_VENCER,C.VENCIDA,C.OBS_VENCIMIENTO,C.DEUDA_SOLES,C.RECLAMO_SOLES,C.DEPARTAMENTO,C.GESTOR_ASIGNADO,C.NOMBRE_CARTERA,C.REGION,
G.F_GESTION,
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN G.TIPO_GESTION ELSE 'SIN GESTION' END)TIPO_GESTION,
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN G.TIPIFICACION ELSE 'SIN GESTION' END)TIPIFICACION,
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN G.ESCENARIO_TIPIFICACION ELSE 'SIN GESTION' END)ESCENARIO_TIPIFICACION,
COALESCE(G.RESULTADO,'SIN GESTION')RESULTADO,
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN (UPPER(TRIM(BOTH ' ' FROM replace(replace(replace(replace(replace(replace(upper(CONVERT(utl_raw.cast_to_varchar2((nlssort(G.MOTIVO_NOPAGO,'nls_sort=binary_ai'))), 'US7ASCII', 'WE8MSWIN1252')),'_',''),' ',''),',',''),'.',''),' ',''),'?','N')))) ELSE 'SIN GESTION'END)MOTIVO_NOPAGO,
G.FCOMPROMISO,COALESCE(G.GESTOR,'SIN GESTION')GESTOR_GESTION
FROM 
(SELECT A.CUST_ACCOUNT,A.RUC_DNI,A.NOMBRE_COMPLETO_CLIENTE,A.TIPO_CORPORATIVO,A.SEGMENTO_COMERCIAL,
SUM(A.TOTAL_A_GESTIONAR_S)TOTAL_A_GESTIONAR_S,
NVL(SUM(D.POR_VENCER),0)POR_VENCER,
NVL(SUM(D.VENCIDA),0)VENCIDA,
D.OBS_VENCIMIENTO,
NVL(SUM(D.SALDO_SOLES),0)DEUDA_SOLES,
NVL(SUM(D.RECLAMO_SOLES),0)RECLAMO_SOLES,
A.DEPARTAMENTO,A.GESTOR_ASIGNADO ,A.NOMBRE_CARTERA,A.REGION FROM 

(SELECT A.CUST_ACCOUNT,A.RUC_DNI,A.NOMBRE_COMPLETO_CLIENTE,
(CASE WHEN A.AFILIADOS_AFP IS NOT NULL THEN A.AFILIADOS_AFP ELSE A.TIPO_CORPORATIVO END)TIPO_CORPORATIVO,
(CASE WHEN (SITIC ='MICRO' OR SITIC = 'PYME' OR SITIC IS NULL)THEN 'NEGOCIOS' ELSE SITIC END)SEGMENTO_COMERCIAL,
A.NRO_DOCUMENTO,SUM(A.TOTAL_A_GESTIONAR_S)TOTAL_A_GESTIONAR_S,A.DEPARTAMENTO,
A.GESTOR GESTOR_ASIGNADO,A.NOMBRE_CARTERA,UPPER(A.REGION)REGION 
--SELECT*
FROM TABLA_ASIGNACION A 
WHERE SEGMENTO IS NULL
AND GESTOR IN ('EMPRESA1','EMPRESA2','EMPRESA3','EMPRESA4','EMPRESA5')
AND (CASE WHEN A.AFILIADOS_AFP IS NOT NULL THEN A.AFILIADOS_AFP ELSE A.TIPO_CORPORATIVO END) NOT IN ('SEGMENTO3')
GROUP BY A.CUST_ACCOUNT,A.RUC_DNI,A.NOMBRE_COMPLETO_CLIENTE,
(CASE WHEN A.AFILIADOS_AFP IS NOT NULL THEN A.AFILIADOS_AFP ELSE A.TIPO_CORPORATIVO END),
SITIC,A.NRO_DOCUMENTO,A.DEPARTAMENTO,A.GESTOR,A.NOMBRE_CARTERA,UPPER(A.REGION))A,

(SELECT CUENTA , NRO_DOC , 
(CASE WHEN VENCIMIENTO>= SYSDATE THEN SUM(SALDO_SOLES) ELSE 0 END)POR_VENCER,
(CASE WHEN VENCIMIENTO< SYSDATE THEN SUM(SALDO_SOLES) ELSE 0 END)VENCIDA,
(CASE WHEN VENCIMIENTO< SYSDATE THEN 'CON DEUDA VENCIDA' ELSE 'SIN DEUDA VENCIDA' END)OBS_VENCIMIENTO,
 SUM(SALDO_SOLES)SALDO_SOLES,SUM(D.RECLAMO_SOLES)RECLAMO_SOLES FROM 
TABLA_DEUDA D
WHERE D.ATTRIBUTE19 IN ('ORIGEN2','ORIGEN1')
AND D.MONTO_FAC>0
AND D.SALDO>0
GROUP BY CUENTA ,NRO_DOC,VENCIMIENTO)D

WHERE 1=1
AND A.CUST_ACCOUNT = D.CUENTA(+)
AND A.NRO_DOCUMENTO = D.NRO_DOC(+)
GROUP BY A.CUST_ACCOUNT,A.RUC_DNI,A.NOMBRE_COMPLETO_CLIENTE,A.TIPO_CORPORATIVO,A.SEGMENTO_COMERCIAL,
A.DEPARTAMENTO,A.GESTOR_ASIGNADO,A.NOMBRE_CARTERA,A.REGION,D.OBS_VENCIMIENTO)C,

(SELECT X.* FROM 
(SELECT /*+ parallel(6) */I.FECHA_INICIO,I.FECHA_FIN,G.F_GESTION,G.RUC_DNI,G.CUSTOMERID,G.NOMBRE_DE_CARTERA,G.GESTOR,G.TIPO_GESTION,G.TIPIFICACION,G.ESCENARIO_TIPIFICACION,G.RESULTADO, G.MOTIVO_NOPAGO,G.FCOMPROMISO,
ROW_NUMBER() OVER (PARTITION BY G.CUSTOMERID ORDER BY G.RESULTADO ASC,G.F_GESTION DESC , G.FCOMPROMISO ASC, G.TIPO_GESTION ASC)RN
FROM TABLA_GESTION_DIARIA G,

(SELECT MIN(FECHA)FECHA_INICIO,
MAX(TRUNC(CASE WHEN FECHA_FIN IS NULL THEN LAST_DAY(SYSDATE) ELSE FECHA_FIN END))FECHA_FIN,CUST_ACCOUNT,
NOMBRE_CARTERA,REGEXP_REPLACE(NOMBRE_CARTERA, SUBSTR(NOMBRE_CARTERA,-6), 'MMM'|| SUBSTR(NOMBRE_CARTERA,-3,1)||'YY')TRAMO_PREF_CARTERA, GESTOR
FROM TABLA_ASIGNACION
WHERE SEGMENTO IS NULL
GROUP BY CUST_ACCOUNT,
NOMBRE_CARTERA,REGEXP_REPLACE(NOMBRE_CARTERA, SUBSTR(NOMBRE_CARTERA,-6), 'MMM'|| SUBSTR(NOMBRE_CARTERA,-3,1)||'YY'), GESTOR)I

WHERE 1=1
AND G.CUSTOMERID=I.CUST_ACCOUNT
AND G.GESTOR=I.GESTOR
AND G.F_GESTION>=I.FECHA_INICIO
AND G.F_GESTION<=I.FECHA_FIN
AND G.NOMBRE_DE_CARTERA=I.NOMBRE_CARTERA
)X WHERE RN=1)G     

  
WHERE C.CUST_ACCOUNT=G.CUSTOMERID(+)
AND C.GESTOR_ASIGNADO=G.GESTOR(+)
GROUP BY
C.CUST_ACCOUNT, C.RUC_DNI,C.NOMBRE_COMPLETO_CLIENTE,C.TOTAL_A_GESTIONAR_S,C.POR_VENCER,C.VENCIDA,C.OBS_VENCIMIENTO,C.DEUDA_SOLES,C.RECLAMO_SOLES,
C.TIPO_CORPORATIVO,C.SEGMENTO_COMERCIAL,C.DEPARTAMENTO,C.GESTOR_ASIGNADO,C.NOMBRE_CARTERA,C.REGION,G.F_GESTION,
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN G.TIPO_GESTION ELSE 'SIN GESTION' END),
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN G.TIPIFICACION ELSE 'SIN GESTION' END),
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN G.ESCENARIO_TIPIFICACION ELSE 'SIN GESTION' END),
COALESCE(G.RESULTADO,'SIN GESTION'),
(CASE WHEN G.TIPO_GESTION IS NOT NULL THEN (UPPER(TRIM(BOTH ' ' FROM replace(replace(replace(replace(replace(replace(upper(CONVERT(utl_raw.cast_to_varchar2((nlssort(G.MOTIVO_NOPAGO,'nls_sort=binary_ai'))), 'US7ASCII', 'WE8MSWIN1252')),'_',''),' ',''),',',''),'.',''),' ',''),'?','N')))) ELSE 'SIN GESTION'END),
G.FCOMPROMISO, COALESCE(G.GESTOR,'SIN GESTION')


  )X
  
GROUP BY  X.TIPO_CORPORATIVO,
X.SEGMENTO_COMERCIAL,
X.OBS_VENCIMIENTO,
X.DEPARTAMENTO,
X.GESTOR_ASIGNADO,
X.NOMBRE_CARTERA,
X.REGION,
X.TIPO_GESTION,
X.TIPIFICACION,
X.ESCENARIO_TIPIFICACION,
X.RESULTADO