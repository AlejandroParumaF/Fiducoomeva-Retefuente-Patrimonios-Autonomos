import oracledb
import pandas as pd
from os import listdir, environ
from os.path import join

class Controller(object):

    def __init__(self):
        self.config_sifi = {
            'user'           : "SIFI_RPT",
            'pwd'            : 'C00m3v4123*',
            'host'           : 'BDCDPORA11.INTRACOOMEVA.COM.CO',
            'port'           :  1554,
            'service_name'   : 'CDPORA11'
            }
        
        self.path = self.find_path('BOT RETEFUENTE PATRIMONIOS AUTONOMOS')
        self.connection = self.create_connection()

    def find_path(self, name):
        user_path = join(environ['USERPROFILE'])

        for fileName in listdir(user_path):
            if fileName == name:
                return(join(path, file))
            try:
                path = join(user_path, fileName)
                for file in listdir(path):
                    if file == name:
                        return(join(path, file))              
                    try:
                        path2= join(path, file)
                        for f in listdir(path2):
                            if f == name:
                                return(join(path2,f))
                        try:
                            path3= join(path2, file)
                            for f2 in listdir(path3):
                                if f2 == name:
                                    return(join(path3, f2))               
                                try:
                                    path4= join(path3, file)
                                    for f3 in listdir(path4):
                                        if f3 == name:
                                            return(join(path4, f3))
                                except:
                                    continue
                        except:
                            continue
                    except:
                        continue
            except:
                continue

    def create_connection(self):
        con_params = oracledb.ConnectParams(host = self.config_sifi['host'], port = self.config_sifi['port'], service_name = self.config_sifi['service_name'])
        return oracledb.connect(user = self.config_sifi['user'],  password = self.config_sifi['pwd'], params = con_params)
     
    def close_connection(self):
        self.connection.close()

    def extract_data_sifi(self, sql, params):
        df = pd.read_sql(sql, self.connection, params = params)
        return df

    def get_consolidated_by_cias(self):
        sql = '''
            select * 
            from(
                select 
                    cias_cias COD, cias_descri NOM, cias_nrorif NIT, SUBSTR(sald_mayo, 1, 4) CTA, sum(sald_salact) SALD 
                from 
                    sc_tsald left join ge_tcias on cias_cias = sald_cias
                where 
                    sald_fecmov = 202501 and cias_nrorif = 901061400 and sald_mayo like '2519%'and sald_mayo between 2519 and 251905059999
                group by 
                    cias_cias, cias_descri, cias_nrorif, SUBSTR(sald_mayo, 1, 4)
            )
            where 
                sald <> 0
            '''
        df = self.extract_data_sifi(sql, [])

        remove_list = [106319, 106318, 106320, 119210, 120267, 120268, 696451]

        for id in remove_list:
            df = df .loc[~(df ['COD'] == id)]

        df['SALD'] = (df['SALD'] / 1000).round().astype(int) * 1000

        return df

    def get_retention_list(self):
        sql = '''

        select
            mvco_mayo AS Cuenta,
            auxi_nit AS Nit_Tercero,
            auxi_descri AS Nombre_Tercero,
            auxi_natu AS TIPO,
            mayo_descri AS Descripcion,
            mvco_cias AS Codigo_Empresa ,
            cias_descri AS Nombre_Empresa,
            SALD_SALANT AS Inicial,
            SUM(mvco_mtoren) AS Valor,
            SALD_SALACT AS Saldo,
            mvco_fecmov AS Periodo,
            mvco_etct AS ETCT
        from SC_TMVCO  
            LEFT JOIN ge_tauxil ON sc_tmvco.mvco_auxi = ge_tauxil.auxi_auxi 
            LEFT JOIN ge_tcias ON sc_tmvco.mvco_cias = ge_tcias.cias_cias 
            LEFT JOIN GE_TMAYOR ON sc_tmvco.mvco_mayo = GE_TMAYOR.mayo_mayo and sc_tmvco.mvco_etct = GE_TMAYOR.mayo_etct
            LEFT JOIN SC_TSALD ON SALD_MAYO = mvco_mayo and SALD_CIAS = mvco_cias and sald_auxi = mvco_auxi and sald_etct = mvco_etct and sald_fecmov = mvco_fecmov
        where 
            MVCO_FECMOV between 202501 and 202501 AND MVCO_MAYO LIKE '2519%' AND LENGTH(MVCO_MAYO) = 12
            and mvco_cias between 719 and 803962
            AND CIAS_NRORIF = 901061400
        GROUP BY
            mvco_mayo,
            auxi_nit,
            auxi_descri,
            auxi_natu,
            mayo_descri,
            mvco_cias,
            cias_descri,
            sald_salant,
            sald_salact,
            mvco_fecmov,
            mvco_etct
        '''

        df = self.extract_data_sifi(sql, [])

        validate_list = ["SERVICIOS TRANSPORTE DE CARGA 1%", "ARRENDAMIENTO BIENES MUEBLES 4%","ARRENDAMIENTO BIENES RAICES 3.5%","COMISIONES 11%","COMPRAS 0.1%","COMPRAS 2.5%","COMPRAS 3.5%","HONORARIOS 10%","HONORARIOS 11%","HONORARIOS 3.5%","OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE","OTROS INGRESOS TRIBUTARIOS DECLARANTES FIDEICOMISOS 2.5%","PAGOS AL EXTERIOR 15%","PAGOS AL EXTERIOR 20%","RENDIMIENTOS FINANCIEROS 2.5%","RENDIMIENTOS FINANCIEROS 4%","RENDIMIENTOS FINANCIEROS 7%","RETEFUENTE IVA PAGOS AL EXTERIOR 19%","RETENCIONES PRACTICADAS EN EXCESO O INDEBIDAS","SERVICIOS 1%","SERVICIOS 2%","SERVICIOS 3.5%","SERVICIOS 4 %","SERVICIOS 6 %"]

        dict = {
            "SERVICIOS TRANSPORTE DE CARGA 1%": {'base': 0.01, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "ARRENDAMIENTO BIENES MUEBLES 4%": {'base': 0.04, 'retencion': 1, 'concepto': "RF ARRENDAMIENTOS"},
            "ARRENDAMIENTO BIENES RAICES 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF ARRENDAMIENTOS"},
            "COMISIONES 11%": {'base': 0.11, 'retencion': 1, 'concepto': "RF COMISIONES"},
            "COMPRAS 0.1%": {'base': 0.001, 'retencion': 1, 'concepto': "RF COMPRAS"},
            "COMPRAS 2.5%": {'base': 0.025, 'retencion': 1, 'concepto': "RF COMPRAS"},
            "COMPRAS 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF COMPRAS"},
            "HONORARIOS 10%": {'base': 0.1, 'retencion': 1, 'concepto': "RF HONORARIOS"},
            "HONORARIOS 11%": {'base': 0.11, 'retencion': 1, 'concepto': "RF HONORARIOS"},
            "HONORARIOS 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF HONORARIOS"},
            "OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE": {'base': 0.025, 'retencion': 1, 'concepto': "OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE"},
            "OTROS INGRESOS TRIBUTARIOS DECLARANTES FIDEICOMISOS 2.5%": {'base': 0.025, 'retencion': 1, 'concepto': "OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE"},
            "PAGOS AL EXTERIOR 15%": {'base': 0.15, 'retencion': 1, 'concepto': "PAGOS AL EXTERIOR 15%"},
            "PAGOS AL EXTERIOR 20%": {'base': 0.2, 'retencion': 1, 'concepto': "PAGOS AL EXTERIOR 20%"},
            "RENDIMIENTOS FINANCIEROS 2.5%": {'base': 0.025, 'retencion': 1, 'concepto': "RF RENDIMIENTOS FINANCIEROS"},
            "RENDIMIENTOS FINANCIEROS 4%": {'base': 0.04, 'retencion': 1, 'concepto': "RF RENDIMIENTOS FINANCIEROS"},
            "RENDIMIENTOS FINANCIEROS 7%": {'base': 0.07, 'retencion': 1, 'concepto': "RF RENDIMIENTOS FINANCIEROS"},
            "RETEFUENTE IVA PAGOS AL EXTERIOR 19%": {'base': 0.19, 'retencion': 1, 'concepto': "RETEFUENTE IVA PAGOS AL EXTERIOR 19%"},
            "RETENCIONES PRACTICADAS EN EXCESO O INDEBIDAS": {'base': 0, 'retencion': -1, 'concepto': "RETENCIONES PRACTICADAS EN EXCESO O INDEBIDAS"},
            "SERVICIOS 1%": {'base': 0.01, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 2%": {'base': 0.02, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 4 %": {'base': 0.04, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 6 %": {'base': 0.06, 'retencion': 1, 'concepto': "RF SERVICIOS"},
        }

        df = df[df['DESCRIPCION'].isin(validate_list)]
        df = df[df["CODIGO_EMPRESA"] != 74840]

        df['SALDO'] = df['VALOR'].astype(float) + df['INICIAL'].astype(float)
        df['VALOR'] = df['VALOR'].astype(float) + df['INICIAL'].astype(float)
        df['VALOR'] = df['VALOR'].abs()
        df['BASE'] = 0
        df['RETENCION'] = 0
        df['CONCEPTO'] = 0

        df.loc[df['NOMBRE_EMPRESA'].str.contains('AVISILVER SOBRECOLATERAL'), 'CODIGO_EMPRESA'] = 106317
        df.loc[df['CODIGO_EMPRESA'] == 106317, 'NOMBRE_EMPRESA'] = 'PA AVISILVER SOBRECOLATERAL - CONSOLIDADORA'

        for  data in validate_list:
            if dict[data]['base'] == 0:
                df.loc[df['DESCRIPCION'] == data, 'BASE'] = 0
            else:
                df.loc[df['DESCRIPCION'] == data, 'BASE'] = df['VALOR'] / dict[data]['base'] 
            df.loc[df['DESCRIPCION'] == data, 'RETENCION'] = df['VALOR'] * dict[data]['retencion'] 
            df.loc[df['DESCRIPCION'] == data, 'CONCEPTO'] = dict[data]['concepto'] 

        df = df.loc[df['SALDO'] != 0]

        df.loc[df['TIPO'] == 'J', 'CONCEPTO_TIPO'] = df['CONCEPTO'] + ' JURIDICA'
        df.loc[df['TIPO'] == 'N', 'CONCEPTO_TIPO'] = df['CONCEPTO'] + ' NATURAL'
        df.loc[df['TIPO'] == 'F', 'CONCEPTO_TIPO'] = df['CONCEPTO'] + ' POR DETERMINAR'
        return df

    def save_in_file(self, df1, df2, filename):  
        with pd.ExcelWriter(join(self.path, filename)) as writer:
            df1.to_excel(writer, sheet_name="Generico_Datos", index=False)
            df2.to_excel(writer, sheet_name="Generico_Consolidado", index=False)

    def read_arrendatarios(self):
        for file in listdir(self.path):
            if 'Movimiento AP' in file:
                filename = file
                break
        
        columns = ['NIT', 'VENDOR_NAME', 'VALOR_DIST', 'CUENTA']

        df = pd.read_excel(join(self.path, filename), sheet_name = 'AP', skiprows = 1, usecols = columns)
        df['NIT'] = df['NIT'].fillna(0).astype(int).astype(str)
        df['CUENTA'] = df['CUENTA'].fillna(0).astype(int).astype(str)
        df['VALOR_DIST'] = df['VALOR_DIST'].astype(float)

        mask = df['CUENTA'].str.startswith('2519')
        df = df[mask]

        df = df.groupby(['NIT', 'VENDOR_NAME', 'CUENTA'])
        df = df['VALOR_DIST'].sum().reset_index()

        df = df.rename(columns = {'NIT':'NIT_TERCERO', 'VENDOR_NAME':'NOMBRE_TERCERO', 'VALOR_DIST':'VALOR'})

        nits = self.get_type_auxi_arrendatarios(df['NIT_TERCERO'].unique().tolist())
        cuentas = self.get_cuenta_descri_arrendatarios(df['CUENTA'].unique().tolist())

        nits['NIT'] = nits['NIT'].fillna(0).astype(int).astype(str)

        for index, cuenta in cuentas.iterrows():
            df.loc[df['CUENTA'] == cuenta['CUENTA'], 'DESCRIPCION'] = cuenta['DESCRIPCION']

        for index, nit in nits.iterrows():
            df.loc[df['NIT_TERCERO'] == nit['NIT'], 'TIPO'] = nit['TIPO']

        validate_list = ["SERVICIOS TRANSPORTE DE CARGA 1%", "ARRENDAMIENTO BIENES MUEBLES 4%","ARRENDAMIENTO BIENES RAICES 3.5%","COMISIONES 11%","COMPRAS 0.1%","COMPRAS 2.5%","COMPRAS 3.5%","HONORARIOS 10%","HONORARIOS 11%","HONORARIOS 3.5%","OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE","OTROS INGRESOS TRIBUTARIOS DECLARANTES FIDEICOMISOS 2.5%","PAGOS AL EXTERIOR 15%","PAGOS AL EXTERIOR 20%","RENDIMIENTOS FINANCIEROS 2.5%","RENDIMIENTOS FINANCIEROS 4%","RENDIMIENTOS FINANCIEROS 7%","RETEFUENTE IVA PAGOS AL EXTERIOR 19%","RETENCIONES PRACTICADAS EN EXCESO O INDEBIDAS","SERVICIOS 1%","SERVICIOS 2%","SERVICIOS 3.5%","SERVICIOS 4 %","SERVICIOS 6 %"]
        df = df[df['DESCRIPCION'].isin(validate_list)]



        df['CODIGO_EMPRESA'] = 74840
        df['NOMBRE_EMPRESA'] = 'PA ARRENDATARIOS'
        df['INICIAL'] = 0
        df['SALDO'] = 0
        df['PERIODO'] = 202501
        df['ETCT'] = 14
        df['BASE'] = 0
        df['RETENCION'] = 0
        df['CONCEPTO'] = ''

        df = df[['CUENTA', 'NIT_TERCERO', 'NOMBRE_TERCERO', 'TIPO', 'DESCRIPCION', 'CODIGO_EMPRESA', 'NOMBRE_EMPRESA', 'INICIAL', 'VALOR', 'SALDO', 'PERIODO', 'ETCT', 'BASE', 'RETENCION', 'CONCEPTO']]

        dict = {
            "SERVICIOS TRANSPORTE DE CARGA 1%": {'base': 0.01, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "ARRENDAMIENTO BIENES MUEBLES 4%": {'base': 0.04, 'retencion': 1, 'concepto': "RF ARRENDAMIENTOS"},
            "ARRENDAMIENTO BIENES RAICES 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF ARRENDAMIENTOS"},
            "COMISIONES 11%": {'base': 0.11, 'retencion': 1, 'concepto': "RF COMISIONES"},
            "COMPRAS 0.1%": {'base': 0.001, 'retencion': 1, 'concepto': "RF COMPRAS"},
            "COMPRAS 2.5%": {'base': 0.025, 'retencion': 1, 'concepto': "RF COMPRAS"},
            "COMPRAS 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF COMPRAS"},
            "HONORARIOS 10%": {'base': 0.1, 'retencion': 1, 'concepto': "RF HONORARIOS"},
            "HONORARIOS 11%": {'base': 0.11, 'retencion': 1, 'concepto': "RF HONORARIOS"},
            "HONORARIOS 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF HONORARIOS"},
            "OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE": {'base': 0.025, 'retencion': 1, 'concepto': "OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE"},
            "OTROS INGRESOS TRIBUTARIOS DECLARANTES FIDEICOMISOS 2.5%": {'base': 0.025, 'retencion': 1, 'concepto': "OTROS INGRESOS TRIBUTARIOS 2.5% DECLARANTE"},
            "PAGOS AL EXTERIOR 15%": {'base': 0.15, 'retencion': 1, 'concepto': "PAGOS AL EXTERIOR 15%"},
            "PAGOS AL EXTERIOR 20%": {'base': 0.2, 'retencion': 1, 'concepto': "PAGOS AL EXTERIOR 20%"},
            "RENDIMIENTOS FINANCIEROS 2.5%": {'base': 0.025, 'retencion': 1, 'concepto': "RF RENDIMIENTOS FINANCIEROS"},
            "RENDIMIENTOS FINANCIEROS 4%": {'base': 0.04, 'retencion': 1, 'concepto': "RF RENDIMIENTOS FINANCIEROS"},
            "RENDIMIENTOS FINANCIEROS 7%": {'base': 0.07, 'retencion': 1, 'concepto': "RF RENDIMIENTOS FINANCIEROS"},
            "RETEFUENTE IVA PAGOS AL EXTERIOR 19%": {'base': 0.19, 'retencion': 1, 'concepto': "RETEFUENTE IVA PAGOS AL EXTERIOR 19%"},
            "RETENCIONES PRACTICADAS EN EXCESO O INDEBIDAS": {'base': 0, 'retencion': -1, 'concepto': "RETENCIONES PRACTICADAS EN EXCESO O INDEBIDAS"},
            "SERVICIOS 1%": {'base': 0.01, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 2%": {'base': 0.02, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 3.5%": {'base': 0.035, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 4 %": {'base': 0.04, 'retencion': 1, 'concepto': "RF SERVICIOS"},
            "SERVICIOS 6 %": {'base': 0.06, 'retencion': 1, 'concepto': "RF SERVICIOS"},
        }

        df['SALDO'] = df['VALOR'].astype(float) + df['INICIAL'].astype(float)
        #df['VALOR'] = df['VALOR'].abs()

        for  data in validate_list:
            if dict[data]['base'] == 0:
                df.loc[df['DESCRIPCION'] == data, 'BASE'] = 0
            else:
                df.loc[df['DESCRIPCION'] == data, 'BASE'] = df['VALOR'] / dict[data]['base'] 
            df.loc[df['DESCRIPCION'] == data, 'RETENCION'] = df['VALOR'] * dict[data]['retencion'] 
            df.loc[df['DESCRIPCION'] == data, 'CONCEPTO'] = dict[data]['concepto'] 

            df = df.loc[df['SALDO'] != 0]


        return df
    
    def get_type_auxi_arrendatarios(self, lista_nits):
        if not lista_nits:
            return pd.DataFrame()

        try:
            placeholders = ', '.join([':' + str(i + 1) for i in range(len(lista_nits))])
            sql = f"""
                SELECT
                    auxi_nit AS NIT,
                    CASE 
                        WHEN auxi_natu = 'N' THEN 'N'
                        WHEN auxi_natu = 'J' THEN 'J'
                        WHEN auxi_natu = 'F' THEN 'F'
                        WHEN auxi_natu = 'P' THEN 'J'
                    END TIPO
                FROM
                    ge_tauxil
                WHERE
                    auxi_nit IN ({placeholders})
            """
            params = {str(i + 1): nit for i, nit in enumerate(lista_nits)}
            df = pd.read_sql(sql, self.connection, params=params)
            return df

        except oracledb.Error as e:
            print(f"Error al ejecutar la consulta: {e}")
            return pd.DataFrame()
    
    def get_cuenta_descri_arrendatarios(self, lista_cuentas):
        if not lista_cuentas:
            return pd.DataFrame()

        try:
            placeholders = ', '.join([':' + str(i + 1) for i in range(len(lista_cuentas))])
            sql = f"""
                select mayo_mayo AS CUENTA, mayo_descri AS DESCRIPCION from GE_TMAYOR
                where mayo_mayo in ({placeholders})
                and mayo_etct = 14
            """
            params = {str(i + 1): nit for i, nit in enumerate(lista_cuentas)}
            df = pd.read_sql(sql, self.connection, params=params)
            return df

        except oracledb.Error as e:
            print(f"Error al ejecutar la consulta: {e}")
            return pd.DataFrame()
    
controller = Controller()
df1 = controller.get_retention_list()
df2 = controller.get_consolidated_by_cias()

controller.save_in_file(df1, df2, 'RETEFUENTE PATRIMONIOS AUTONOMOS.xlsx')

#df = controller.read_arrendatarios()
controller.close_connection()
#df.to_excel('arrendatarios.xlsx', index=False)



