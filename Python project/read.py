from fastapi import File
import pandas as pd
import bibtexparser
import requests
import useful
from io import StringIO

#Função para leitura de dados BibTex
async def bib(bibtex_file: File) -> pd.DataFrame:
    
    #Leitura do arquivo no formato bibtex e passando para um dataframe
    bibtex_database = bibtexparser.loads(await bibtex_file.read())     
       
    df = pd.DataFrame(bibtex_database.entries)
    
    return df


#######################################################################################


async def csv(file: File, name_file) -> pd.DataFrame:
    
    #Convertendo o arquivo em bytes para string
    file = str(await file.read(),'utf-8') 
    
    #Transformando em file object
    file = StringIO(file)
    
    #Leitura do arquivo csv
    if name_file == 'jcs': 
        df = pd.read_csv(file, sep=';')
    else:
        df = pd.read_csv(file, sep=';', quotechar='"', low_memory=False)
        
    return df


#######################################################################################


#Função para leitura e padronização dos dados da api do science direct 
def api_scienceDirect(search: str, api_key: str) -> pd.DataFrame:
    
    #Tratamento nos caracteres especiais, passando para o formato hexadecimal
    search_conv =  useful.hex_letter(search)

    request_sc = requests.get(f"https://api.elsevier.com/content/search/sciencedirect?query={search_conv}&apiKey={api_key}")
 
    if request_sc.status_code != 200:     
        raise Exception("Science Direct api request error! Code Status " + str(request_sc.status_code))
    
    request_sc = request_sc.json()
    
    api_science_direct = request_sc['search-results']['entry']
    
    #Lendo os dados no dataframe
    df_sc_api = pd.DataFrame(api_science_direct)

    return df_sc_api


#######################################################################################


#Função para leitura e padronização dos dados da api do ieee 
def api_ieee(search: str, api_key: str) -> pd.DataFrame:
    
    #Tratamento nos caracteres especiais, passando para o formato hexadecimal
    search_conv =  useful.hex_letter(search)
    
    #Realizando a requisição
    request_ieee = requests.get(f"http://ieeexploreapi.ieee.org/api/v1/search/articles?querytext={search_conv}&format=json&apikey={api_key}")

    if request_ieee.status_code != 200:
        raise Exception("IEEE api request error! Code Status " + str(request_ieee.status_code))
        
    #Convertendo para json para que o resultado da requisição possa 
    #ser lido em um dataframe
    request_ieee = request_ieee.json()

    #Passando para uma var apenas os artigos
    api_ieee = request_ieee['articles']

    #Lendo os dados no dataframe
    df_ieee_api = pd.DataFrame(api_ieee)
    
    return df_ieee_api