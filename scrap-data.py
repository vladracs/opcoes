import os
import time
import calendar
import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime, timedelta

# connect to MetaTrader 5
if not mt5.initialize():
    print('initialize() failed')
    mt5.shutdown()
 
# get connection status and parameters
print(mt5.terminal_info())

# get MetaTrader 5 version
print(mt5.version())

# path to CSVs
path_to_csvs = 'C:/Users/YourUserName/Desktop/ticks/'

# path to temp data (so we can delete it)
path_to_tmp = 'C:/Users/YourUserName/AppData/Roaming/MetaQuotes/Terminal/HASH/bases/BrokerID/'

# get all B3 tickers
symbols = mt5.symbols_get()

# keep only tickers for ordinary stocks
tickers = []
symbols=mt5.symbols_get()

if symbols is not None:
  
 filtered_symbol = [symbol.name for symbol in symbols if symbol.name.startswith("PETR")]
 for symbol_name in filtered_symbol:
    tickers.append(symbol_name)

# month-years to scrape
months = {
    2019: (9, 10, 11,12),
    2023: range(1,3),
    2024: range(1,3)
}

# loop through tickers
start = time.time()
for i, ticker in enumerate(tickers):

    # loop through month-years
    for year in months.keys():
        for month in months[year]:
            print(' ')
            print(i, 'of', len(tickers), ticker, year, month)

            # set date range
            t0 = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            t1 = datetime(year, month, last_day)

            # request tick data
            ticks = mt5.copy_ticks_range(
                ticker, 
                t0, 
                t1, 
                mt5.COPY_TICKS_TRADE
                )
            ticks = pd.DataFrame(ticks)

            # log if results are empty
            if ticks.shape[0] == 0:
                with open('log.txt', mode = 'a') as f:
                    l = ticker + ',' + str(year) + ',' + str(month) + '\n'
                    f.write(l)
                    print('empty DataFrame:', l)
                    continue

            # persist
            print(ticks.shape[0])
            ticks['time'] = pd.to_datetime(ticks['time'], unit = 's')
            ticks.columns = [
                'ticktime',
               # 'bid',
               # 'ask',
                'last',
                'volume',
                #'time_msc',
                'flags',
                #'volume_real'
                ]
            ticks.to_csv(path_to_csvs + ticker, index = False)

            # don't over-request
            time.sleep(2.5)

    # delete tmp files
    for fname in os.listdir(path_to_tmp + ticker + '/'):
        try:
            os.remove(path_to_tmp + ticker + '/' + fname)
        except:
            pass

    # how long did it take?
    elapsed = time.time() - start
    print('it took', round(elapsed / 60), 'minutes')

# shut down connection to MetaTrader 5
mt5.shutdown()
