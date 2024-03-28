import csv
from datetime import date, time, datetime, timezone
import os
import MetaTrader5 as mt5
from concurrent.futures import ThreadPoolExecutor
import sys

def fetch_data_for_basis(target_temp, symbols):
    """
    Fetch data for all symbols related to a basis_symbol.
    Each basis_symbol gets its own CSV file.
    """
    csv_file_path = os.path.join('E:/_Base_ticks/', f"{target_temp}_trades.csv")
    targets=[]
    #print(target_temp)
    for symbol in symbols:
                #print(symbol.basis)
                if symbol.basis == target_temp and 'OPCOES' in symbol.path:
                    #print(symbol.name)
                    targets.append(symbol.name)  
    #print(targets)

    for target in targets:
        #print(target)

        symbol_info_dict = mt5.symbol_info(target)._asdict()
  
        basis = symbol_info_dict['basis'].strip()
        option_mode_value = symbol_info_dict['option_mode'] # int
        option_right_value = symbol_info_dict['option_right'] # int
        option_strike_value = symbol_info_dict['option_strike']
        option_expiration_time_value = symbol_info_dict['expiration_time']
        symbol_path = symbol_info_dict['path'].strip()

        option_mode = None
        option_right = None
        option_strike = None
        option_expiration = None
        dte = None
        option_mode = 'A' if option_mode_value == 1 else 'E'
        option_right = 'PUT' if option_right_value == 1 else 'CALL'
        option_strike = option_strike_value
        option_expiration_time_dt = datetime.fromtimestamp(option_expiration_time_value)
        option_expiration = option_expiration_time_dt.strftime('%Y-%m-%d')

        # Fetch data for the last hour
        #date_to = datetime.now().replace(tzinfo=timezone.utc)  # Ensure datetime is timezone-aware
        #date_from = date_to - timedelta(hours=4)
        date_from = datetime(2024, 3, 27, hour=1)
        date_to = datetime(2024, 3, 27, hour=23)

        
        candles = mt5.copy_rates_range(target, mt5.TIMEFRAME_M15, date_from, date_to)
        #print(candles)
        if candles is None:
            print(f"\n{target} has 0 candles\n")
            continue

        for candle in candles:


            t  = int(candle[0])
            c  = float(candle[4])
            volume_real = int(candle[7])   # real_volume
            #print(t," ",c," ",volume_real)
            t_dt = datetime.fromtimestamp(t, tz=timezone.utc)
            t_ts = t_dt.strftime('%Y-%m-%d %H:%M')
            
            volume_cash = c * volume_real
            #print(volume_cash)
            if volume_cash > 500000:
                message=f'{target}, {basis}, {option_right}, Strike = {option_strike}, Exp = {option_expiration}, {t_ts} ,Preço {c},Volume cash R$ {int(volume_cash):,}'
                send_telegram_message(message,"1591875794",'7182990746:AAF_0Yjb9MixxG2xeP7per5IHYWVVW8ihdY')
        
               # csv_data=(f'{target}, {basis}, {option_right}, {option_mode}, Strike = {option_strike}, Exp = {option_expiration}, {t_ts} , c={c:.2f}, Volume R$ {int(volume_cash):,}')
                #print(csv_data)
                #csvwriter.writerow([csv_data])

def main():
    start_time = datetime.now()
    if not mt5.initialize():
        print("initialize() failed")
        mt5.shutdown()
        return

    symbols = mt5.symbols_get()
    basis_symbols = []

    # Load basis symbols from file or command line
    with open('b3_symbols.txt', 'r') as f:
        basis_symbols = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        print(basis_symbols)
    # Ensure the output directory exists
    os.makedirs('E:/_Base_ticks/', exist_ok=True)

    # Use ThreadPoolExecutor to handle each basis symbol in parallel
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_data_for_basis, basis, symbols) for basis in basis_symbols]
    end_time = datetime.now()
    delta_time = end_time - start_time
    finished_in = f"-- Finished in {delta_time.days} days, {delta_time.seconds // 3600} hours, {delta_time.seconds // 60 % 60} mins, {delta_time.seconds % 60} secs"
    print(finished_in)
    sys.exit(0)


if __name__ == '__main__':
    main()
