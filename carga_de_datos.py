import os
from TradingviewData import TradingViewData, Interval

# Diccionario de criptomonedas con sus respectivos símbolos
crypto_symbols = {
    "Bitcoin": "BTCUSD",
    "Ethereum": "ETHUSD",
    "Ripple": "XRPUSD",
    "Solana": "SOLUSD",
    "Dogecoin": "DOGEUSD",
    "Cardano": "ADAUSD",
    "Shiba Inu": "SHIBUSD",
    "Polkadot": "DOTUSD",
    "Aave": "AAVEUSD",
    "Stellar": "XLMUSD",
}


def fetch_and_save_crypto_data():
    # Crear la carpeta 'data' si no existe
    os.makedirs("data", exist_ok=True)

    trading_view = TradingViewData()

    for crypto_name, symbol in crypto_symbols.items():
        print(f"Descargando datos históricos de {crypto_name}...")

        historical_data = trading_view.get_hist(
            symbol, exchange="COINBASE", interval=Interval.daily, n_bars=4 * 365
        )

        total_data_points = len(historical_data)
        days_per_year = 365

        # Dividir los datos en conjuntos de un año
        yearly_data_splits = [
            (historical_data[i : i + days_per_year], historical_data.index[i].year)
            for i in range(0, total_data_points, days_per_year)
        ]

        # Guardar los datos en archivos CSV por año dentro de la carpeta 'data'
        for yearly_data, year in yearly_data_splits:
            file_name = os.path.join(
                "data", f"{crypto_name}_historical_data_{year}.csv"
            )
            yearly_data.to_csv(file_name)
            print(
                f"Datos de {crypto_name} para el año {year} guardados en {file_name}."
            )


if __name__ == "__main__":
    fetch_and_save_crypto_data()
