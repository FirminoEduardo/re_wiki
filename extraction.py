import pandas as pd
import requests
import json
import os
from datetime import date
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
palavras_excluir = ["demo",
"beta",
"teaser",
"1-shot",
"vr",
"kitchen",
"siege",
"onslaught",
"4th survivor",
"episode",
"arcade",
"collection",
"bundle",
"archives",
"demake",
".NET"]
capcom = "Capcom"
hoje = date.today()

if (response.status_code == 200):
    data = response.json()
    print("Conectado com sucesso a API")

    while (data['next']):
        for game in data.get("results", []):
            if (game.get('released') != None and date.fromisoformat(game.get('released', None)) > hoje):
                continue

            if any(palavra.lower() in game['name'].lower() for palavra in palavras_excluir):
                continue
            
            games_id = game.get('id', [])
            detail = requests.get(f"https://api.rawg.io/api/games/{games_id}", params=params)
            
            if (detail.status_code == 200):
                detail_data = detail.json()
                publishers = detail_data['publishers']
                developers = detail_data['developers']

                if not (capcom.lower() in [item['name'].lower() for item in publishers] or capcom.lower() in [item['name'].lower() for item in developers]):
                    continue
                games.append({  "nome": game.get('name', []), 
                            "data_lancamento": game.get('released', []),
                            "Gênero": [item['name'] for item in game.get('genres', [])],
                            "Plataforma": [item['platform']['name'] for item in game.get('platforms', [])],
                            "Publicadora": [item['name'] for item in publishers],
                            "Desenvolvedora": [item['name'] for item in developers],
                            "Nota média - usuários": game.get('rating', None),
                            "Quantidade de avaliações": game.get('ratings_count', None),
                            "Metacritic": game.get('metacritic', None),
                            "Tempo médio de jogo": game.get('playtime', None),
                            "Classificação etária": (game.get('esrb_rating') or {}).get('name', None)
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