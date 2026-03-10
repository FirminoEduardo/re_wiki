import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

params = {
    "key": API_KEY,
    "search": "Resident Evil",
    "page_size": 30,
    "developers": "capcom"
}

response = requests.get(BASE_URL, params=params)

games = []

if (response.status_code == 200):
    data = response.json()
    print("Conectado com sucesso a API")

    while (data['next']):
        for game in data.get("results", []):
            games_id = game.get('id', [])
            detail = requests.get(f"https://api.rawg.io/api/games/{games_id}", params=params)
            if (detail.status_code == 200):
                detail_data = detail.json()
                publishers = detail_data['publishers']
                developers = detail_data['developers']

                games.append({  "nome": game.get('name', []), 
                            "data_lancamento": game.get('released', []),
                            "Gênero": game.get('genres', []),
                            "Plataforma": game.get('platforms', []),
                            "Publicadora": [item['name'] for item in publishers],
                            "Desenvolvedora": [item['name'] for item in developers]
                        })
                
                print(f"Processando: {game['name']}")
            else:
                print("Não foi possível acessar ID's")
        next_page = requests.get(data['next'])
        data = next_page.json()
else: 
    print(f"Não foi possível conectar a API. ERRO: {response.status_code}")

df = pd.DataFrame(games)

df.to_parquet('jogos.parquet')

print("Extração feita com sucesso!")

print(df)