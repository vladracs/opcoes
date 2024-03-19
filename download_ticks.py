import MetaTrader5 as mt5
import pandas as pd
import datetime
import pytz
import os
import requests

# Configurando ambiente
timezone = pytz.timezone("Etc/UTC")
root_folder = 'C:/Users/BLA/Desktop/HIST_DATA/_Base_ticks/'

# Variáveis para obtenção de dados tick a tick
# Lista IBOV por ordem de participação do índice



start_date = datetime.datetime(2020, 1, 1, tzinfo=timezone)
end_date = datetime.datetime.today().replace(tzinfo=timezone)

# Função que busca dados de ticks de determinado ativo em uma data e armazena em um arquivo csv
def getTickData(asset, date, filepath):
    utc_start = date + datetime.timedelta(hours=9)
    utc_end = date + datetime.timedelta(hours=20)

    tick_data = mt5.copy_ticks_range(asset, utc_start, utc_end, mt5.COPY_TICKS_INFO)
    if len(tick_data) != 0:
        print("Qtde de ticks: ",len(tick_data), "\t Arquivo: ", filepath)

        df = pd.DataFrame(tick_data)
        df['time']=pd.to_datetime(df['time_msc'], unit='ms')
        df.to_csv(filepath, sep=";", index=False)

# Testa se consegue conectar a aplicação do MT5
if not mt5.initialize():
    print('Conexão falhou. Código de erro: ', mt5.last_error())
    quit()
else:

    lista_opcoes_vencimentos=[]
    symbols=mt5.symbols_get()
    
    if symbols is not None:
      
     filtered_symbol = [symbol.name for symbol in symbols if symbol.name.startswith("PETR")]
     for symbol_name in filtered_symbol:
        lista_opcoes_vencimentos.append(symbol_name)
    asset_list=lista_opcoes_vencimentos        
#asset_list = [
#'VALE3',
#'ITUB4',
#'PETRA142']
   # print(asset_list)

    
    for asset in asset_list:
        #print(asset)
        # Criar pasta do ativo se não existir
        assetTickDataPath = root_folder + asset + '/'
        isExist = os.path.exists(assetTickDataPath)
        if not isExist:
            os.makedirs(assetTickDataPath)

        symbol_sel = mt5.symbol_select(asset,True)
        if not symbol_sel:
           # print('Ativo não pode ser selecionado: ', asset)
            mt5.shutdown()
        else:
            hist_date = start_date
            # Percorre todos os dias desde a data início até a data fim, excluindo fim de semana
            while hist_date <= end_date:
                if hist_date.weekday() not in (5,6):
                    # Checa se já tem arquivo com dados, se não tiver contínua
                    file = str(hist_date.year) + "{:02d}".format(int(hist_date.month)) + "{:02d}".format(int(hist_date.day)) + '.csv'
                    if (not os.path.exists(assetTickDataPath + file)):
                        getTickData(asset,hist_date, assetTickDataPath + file)
                hist_date = hist_date + datetime.timedelta(days=1)

    mt5.shutdown()
