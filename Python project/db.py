import pandas as pd
import sqlalchemy as sa
import urllib

def export_db(df: pd.DataFrame, server: str, database: str, username: str, password: str):
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                         "SERVER="+server+";"
                                         "DATABASE="+database+";"
                                         "UID="+username+";"
                                         "PWD="+password+";")

    #Cria a conexão com o banco de dados
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

    #Especifica as colunas que serão utilizadas
    data = pd.DataFrame(df, columns = ['Sourceid', 'title', 'type_publication', 
            'issn', 'SJR', 'SJR_Best_Quartile', 'H_index', 'Total_Docs_2020', 
            'Total_Docs_3years', 'Total_Refs', 'Total_Cites_3years', 
            'Citable_Docs_3years', 'Cites_Doc_2years', 'Ref_Doc', 'Country', 
            'Region', 'publisher1', 'Coverage', 'Categories', 'Total_Cites', 
            'Journal_Impact_Factor', 'Eigenfactor_Score', 'series', 'location', 
            'keywords', 'numpages', 'pages', 'booktitle', 'abstract', 'doi', 
            'url', 'address', 'isbn', 'year', 'author', 'ID', 'articleno', 
            'month', 'journal', 'number', 'volume', 'issue_date', 'note', 
            'edition', 'editor', 'rank', 'content_type', 'article_number',
            'pdf_url', 'html_url', 'abstract_url', 'publication_title', 
            'conference_location', 'conference_dates', 'publication_number',
            'is_number', 'publication_date', 'start_page', 'end_page', 
            'citing_paper_count', 'citing_patent_count', 'issue', 'affiliation', 
            'authorUrl', 'id_author', 'author_order', 'load_date', 'creator'
            'publication_name', 'cover_date', 'starting_page', 'pii', 'ending_page', 'id_search'])            

    #Exporta para a tabela no SQL
    data.to_sql("tb_exportdb", engine, schema="dbo", if_exists="append", index=False)

    #Fecha a conexão
    engine.dispose()
    

 #######################################################################################
    
    
def read_db(id_search: str, server: str, database: str, username: str, password: str) -> pd.DataFrame:
    
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                         "SERVER="+server+";"
                                         "DATABASE="+database+";"
                                         "UID="+username+";"
                                         "PWD="+password+";")
    #Cria a conexão com o banco de dados
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    
    #Construindo a query da consulta
    query = f'SELECT * FROM dbo.tb_exportdb WHERE id_search = \'{id_search}\''
    
    #Realizando a consulta no banco baseado, e armazenando em um dataframe
    df = pd.read_sql(query, engine)

    #Fecha a conexão
    engine.dispose()
    
    return df