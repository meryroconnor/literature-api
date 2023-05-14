import requests
import pandas as pd

base_url = "https://gutendex.com/books"
url = f"{base_url}?author_year_start=2000"

response = requests.get(url)
data = response.json()

autor = []
autor_nacimiento = []
autor_muerte = []
libro = []
genero = []
download_count = []


for i in range(0,32):
    autor.append(data["results"][i]["authors"][0]["name"])
    autor_nacimiento.append(data["results"][i]["authors"][0]["birth_year"])
    autor_muerte.append(data["results"][i]["authors"][0]["death_year"])
    libro.append(data["results"][i]["title"])
    #genero.append(data["results"][i]["bookshelves"])
    download_count.append(data["results"][i]["download_count"])

df = pd.DataFrame({"autor":autor,"nacimiento":autor_nacimiento
                ,"muerte":autor_muerte,"titulo":libro,"download_count":download_count})
print(df)


#["authors"]
#
#
#
#URL definicion
#Request
#cargar_en_redshift
