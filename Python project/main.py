from fastapi import FastAPI, UploadFile
import pandas as pd
import read
import treatment
import filters
import db
import uuid

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Hello Guys"}


@app.get("/query")
def query(id_search: str, type_save: str = 'csv ou json', server: str = 'localhost', database: str = 'PythonETL', 
          username: str = 'python', password: str = '123456'):
    try:
        df_query = db.read_db(id_search, server, database, username, password)

        if "JSON" in type_save.upper(): 
            export = df_query.to_json(orient = 'records')
        elif "CSV" in type_save.upper():
            export = df_query.to_csv(sep='|', encoding='utf-8', header='true')
        else:
             export = {"message: Export type not supported, choose from the following available options: csv or json"}

        return export
    
    except Exception as e:
        return {"Error message": repr(e).replace("Exception(\'", '').replace("\')", '')}


@app.post("/project")
async def create_book(ieee_file: UploadFile, scienceDirect_file: UploadFile, acm_file: UploadFile, scimago_file: UploadFile, 
                      jcs_file: UploadFile, search_api: str, api_key_sciencedirect: str, api_key_ieee: str, 
                      server: str = 'localhost', database: str = 'PythonETL', username: str = 'python', 
                      password: str = '123456', filter_year: str = '', filter_title: str = ''):
    try:
        #Leitura dos arquivos bibtex, csv e api
        df_ieee_read = await read.bib(ieee_file)                                            #IEEE
        df_acm_read = await read.bib(acm_file)                                              #ACM
        df_ScienceDirect_read = await read.bib(scienceDirect_file)                          #Science Direct
        df_jcs_read = await read.csv(jcs_file, 'jcs')                                       #JCS
        df_scimago_read = await read.csv(scimago_file, 'scimago')                           #Scimago
        df_api_sc_read = read.api_scienceDirect(search_api, api_key_sciencedirect)          #API Science Direct
        df_api_ieee_read = read.api_ieee(search_api, api_key_ieee)                          #API IEEE
        
        #Tratamento e padronização dos dados
        df_ieee = treatment.ieee(df_ieee_read)                                              #IEEE   
        df_acm = treatment.acm(df_acm_read)                                                 #ACM
        df_ScienceDirect = treatment.science_direct(df_ScienceDirect_read)                  #Science Direct
        df_jcs = treatment.jcs(df_jcs_read)                                                 #JCS
        df_scimago = treatment.scimago(df_scimago_read)                                     #Scimago
        df_api_sc = treatment.api_scienceDirect(df_api_sc_read)                             #API Science Direct
        df_api_ieee = treatment.api_ieee(df_api_ieee_read)                                  #API IEEE 
        
        #Join entre os arquivos csv, com tratamento pós join
        df_csv = treatment.join_csv(df_jcs, df_scimago)
        
        #União dos arquivos bibtex, csv e api's, e realizando tratamento
        df_end = treatment.union_files(df_acm, df_ieee, df_ScienceDirect, df_api_sc, df_api_ieee, df_csv)
        
        #Filtrando dados
        df_end = filters.dataframe_filter(df_end, filter_year, filter_title)
        
        #Gerando id da consulta e criando uma coluna que irá receber o id
        id_search = str(uuid.uuid4())
        df_end["id_search"] = pd.Series([id_search for x in range(len(df_end.index))])
        
        #Exportando a consulta para o banco de dados sql server
        db.export_db(df_end, server, database, username, password)
        
        #Convertendo para json os dados para retornar na api
        export_json = df_end.to_json(orient = 'records')
              
        return { "id_search": str( id_search ), "Output": export_json } 
    except Exception as e:
        return {"Error message": repr(e).replace("Exception(\'", '').replace("\')", '')}