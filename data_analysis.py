import pyodbc
import pandas as pd
from datetime import datetime

def validar_datas(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date, "%d %b, %Y")
        end_date = datetime.strptime(end_date, "%d %b, %Y")
        if start_date >= end_date:
           raise ValueError("A data inicial deve ser anterior à data final.")
        return start_date, end_date
    except ValueError as e:
        raise ValueError(f"Erro ao validar datas: {e}")

def conectar_sql_server():
    #Configurações de conexão
    server = 'seu_servidor'  #coloque o seu servidor
    database = 'bitcoin_analysis'  #coloque o seu banco de dados
    username = 'seu_usuario' #coloque o seu usuario
    password = 'sua_senha'  #coloque sua senha
    
    cnxn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
    )
    return cnxn

def executar_consulta(cnxn, query):
    cursor = cnxn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows, cursor.description

def carregar_dados_sql_server(cnxn, query):
    df = pd.read_sql_query(query, cnxn)
    return df

def calcular_retornos(df):
    df['retorno_diario'] = df['close'].pct_change()
    df['retorno_acumulado'] = (1 + df['retorno_diario']).cumprod() - 1
    return df

def calcular_medias_moveis(df, periodos=[7, 30, 200]):
    for periodo in periodos:
        df[f'mm_{periodo}'] = df['close'].rolling(window=periodo).mean()
    return df

def calcular_volatilidade(df, periodo=30):
    df[f'volatilidade_{periodo}'] = df['close'].rolling(window=periodo).std()
    return df

def adicionar_dia_semana(df):
    df['dia_da_semana'] = df['timestamp'].dt.day_name()
    return df

def preparar_dados_para_powerbi(df):
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    df = df.sort_values(by='timestamp')
    return df

if __name__ == '__main__':
    try:
        # Validar datas
        start_date_str = "1 Jan, 2019"
        end_date_str = "01 Jan, 2024"
        validar_datas(start_date_str, end_date_str)

        #Conectar ao SQL Server
        cnxn = conectar_sql_server()

        #Consultar dados
        query = "SELECT * FROM bitcoin_prices ORDER BY timestamp ASC" #Ordenar os dados por data
        df = carregar_dados_sql_server(cnxn, query)
    
        #Calcular Retornos
        df = calcular_retornos(df)
    
        #Calcular Médias Móveis
        df = calcular_medias_moveis(df)

        #Calcular Volatilidade
        df = calcular_volatilidade(df)

        #Adicionar Dia da Semana
        df = adicionar_dia_semana(df)
        
        #Preparar os dados para o Power BI
        df = preparar_dados_para_powerbi(df)

        #Salvar os dados
        df.to_csv('bitcoin_data_analisado.csv', index=False)
        print("Dados analisados e salvos em 'bitcoin_data_analisado.csv'")

    except pyodbc.Error as ex:
      sqlstate = ex.args[0]
      print(f"Erro ao conectar com SQL Server: {sqlstate}")
    except ValueError as e:
       print(f"Erro de validação: {e}")
    except Exception as e:
      print(f"Erro ao processar dados: {e}")
    finally:
        if cnxn:
            cnxn.close()