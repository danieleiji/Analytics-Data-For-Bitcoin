import pandas as pd
from binance.client import Client
import os
from datetime import datetime
from dotenv import load_dotenv # Importar dotenv

load_dotenv() # Carregar variáveis do arquivo .env

def validar_datas(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date, "%d %b, %Y")
        end_date = datetime.strptime(end_date, "%d %b, %Y")
        if start_date >= end_date:
           raise ValueError("A data inicial deve ser anterior à data final.")
        return start_date, end_date
    except ValueError as e:
        raise ValueError(f"Erro ao validar datas: {e}")

def obter_dados_binance(symbol, start_date, end_date, interval=Client.KLINE_INTERVAL_1DAY, cache_dir="data/raw"):
    # Ler credenciais das variáveis de ambiente
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    # Validar se as variáveis foram carregadas
    if not api_key or not api_secret:
        raise ValueError("As variáveis de ambiente BINANCE_API_KEY e BINANCE_API_SECRET não foram definidas.")


    #Validar datas
    start_date, end_date = validar_datas(start_date, end_date)
    start_date_str = start_date.strftime("%d %b, %Y")
    end_date_str = end_date.strftime("%d %b, %Y")

    cache_filename = f"{symbol}_{start_date_str.replace(' ', '_')}_{end_date_str.replace(' ', '_')}_{interval}.csv"
    cache_path = os.path.join(cache_dir, cache_filename)

    if os.path.exists(cache_path):
        print(f"Carregando dados de cache: {cache_path}")
        df = pd.read_csv(cache_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    try:
        client = Client(api_key, api_secret)
        klines = client.get_historical_klines(symbol, interval, start_date_str, end_date_str)
    except Exception as e:
        raise Exception(f"Erro ao obter dados da Binance: {e}")
    
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    df['timestamp'] = pd.to_datetime(df['timestamp']/1000, unit='s')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    
    # Salvar em cache
    os.makedirs(cache_dir, exist_ok=True)
    df.to_csv(cache_path, index=False)
    print(f"Dados salvos em cache: {cache_path}")

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

if __name__ == '__main__':
    symbol = "BTCUSDT"
    start_date = "1 Jan, 2019"
    end_date = "09 Jan, 2025"
    intervalo = Client.KLINE_INTERVAL_1DAY

    try:
      bitcoin_data = obter_dados_binance(symbol, start_date, end_date, intervalo)
      bitcoin_data = calcular_retornos(bitcoin_data)
      bitcoin_data = calcular_medias_moveis(bitcoin_data)
      bitcoin_data = calcular_volatilidade(bitcoin_data)
      print(bitcoin_data.head())
      
      # Salvar na pasta de dados processados
      output_path = 'data/processed/bitcoin_data_analise.csv'
      bitcoin_data.to_csv(output_path, index=False)
      print(f"Dados e análise salvos em {output_path}")
    except ValueError as e:
      print(f"Erro de validação: {e}")
    except Exception as e:
      print(f"Erro ao processar dados: {e}")