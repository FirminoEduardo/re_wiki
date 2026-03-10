import pandas as pd

df = pd.read_parquet('jogos.parquet')

df['data_lancamento'] = pd.to_datetime(df['data_lancamento'])

df = df.sort_values(by='data_lancamento') 

df['data_lancamento'] = df['data_lancamento'].dt.strftime(date_format='%d/%m/%Y')

print(df)