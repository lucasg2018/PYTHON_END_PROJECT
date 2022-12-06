import pandas as pd
import numpy as np
import useful

#Função para tratamento e padronização do arquivo bibtex do ieee 
def ieee(df: pd.DataFrame) -> pd.DataFrame:
    df_ieee = df.copy()
    
    #Renomeando coluna
    df_ieee.rename(columns={'ENTRYTYPE':'type_publication', 'ISSN':'issn'}, inplace=True)
    
    #Removendo '-' da coluna issn
    df_ieee['issn'].replace('-', '', regex=True, inplace=True)
    
    return df_ieee


#######################################################################################


#Função para tratamento e padronização do arquivo bibtex do acm 
def acm(df: pd.DataFrame) -> pd.DataFrame:
    df_acm = df.copy()
    
    #Renomeando coluna
    df_acm.rename(columns={'ENTRYTYPE':'type_publication'}, inplace=True)

    #Removendo '-' da coluna issn
    df_acm['issn'].replace('-', '', regex=True, inplace=True)
    
    return df_acm


#######################################################################################


#Função para tratamento e padronização do arquivo bibtex do science direct 
def science_direct(df: pd.DataFrame) -> pd.DataFrame:
    df_ScienceDirect = df.copy()
    
    df_ScienceDirect.rename(columns={'ENTRYTYPE':'type_publication'}, inplace=True)

    #Removendo '-' da coluna issn
    df_ScienceDirect['issn'].replace('-', '', regex=True, inplace=True)
    
    return df_ScienceDirect


#######################################################################################


#Função para tratamento e padronização do arquivo csv JCS
def jcs(df_jcs: pd.DataFrame) -> pd.DataFrame:
    
    #Dropando colunas não utilizadas
    df_jcs.drop(["Rank", "Unnamed: 3", "Unnamed: 6", "Unnamed: 7"], axis=1, inplace=True)

    #Renomeando coluna
    df_jcs.rename(columns={'Full Journal Title':'title'}, inplace=True)

    #Removendo as linhas duplicadas
    df_jcs.drop_duplicates(keep='first',inplace=True)

    #Criando coluna hash baseado na coluna title
    df_jcs = useful.make_str_key(df_jcs, 'title')
    
    return df_jcs


#######################################################################################


#Função para tratamento e padronização do arquivo csv scimago
def scimago(df_scimago: pd.DataFrame) -> pd.DataFrame:
    
    #Dropando colunas não utilizadas
    df_scimago.drop(["Rank"], axis=1, inplace=True)

    #Renomeando coluna
    df_scimago.rename(columns={'Issn':'issn_full','Title':'title', 'Type':'type_publication', 
                               'Publisher':'publisher'}, inplace=True)

    #Criando uma coluna nova para receber os issn's existentes dentro do issn_full
    df_scimago['issn'] = df_scimago['issn_full'].str.split(', ')
    df_scimago = df_scimago.explode('issn')

    #Removendo as linhas duplicadas
    df_scimago.drop_duplicates(keep='first',inplace=True)

    #Criando coluna hash baseado na coluna title
    df_scimago = useful.make_str_key(df_scimago, 'title')
    
    return df_scimago


#######################################################################################


#Função para leitura e padronização dos dados da api do science direct 
def api_scienceDirect(df_sc_api: pd.DataFrame) -> pd.DataFrame:
    
    #Trantando coluna de autores que estava em formato de dicionario, com vários atores, foi realizado o apply
    #e explode para trazer os dados contidos no interior dos dicionarios, tornando eles em colunas.
    df_sc_api = pd.concat([df_sc_api.drop(['authors'], axis=1), 
                             df_sc_api['authors'].apply(pd.Series)], axis=1)
    df_sc_api = df_sc_api.explode('author')
    df_sc_api = pd.concat([df_sc_api.drop(['author'], axis=1), 
                             df_sc_api['author'].apply(pd.Series)], axis=1)
    
    #Renomeando coluna
    df_sc_api.rename(columns={'load-date': 'load date', 'prism:url':'url', 'dc:title': 'title', 
        'dc:creator': 'creator', 'prism:publicationName':'publication name','prism:doi':'doi',
        'prism:volume': 'volume', 'prism:coverDate':'cover date', 'prism:endingPage': 'ending page', 
        'prism:startingPage':'starting page', '$':'author'}, inplace=True)
    
    #Selecionando as colunas para evitar possiveis retornos inesperados da api
    df_sc_api = pd.DataFrame(df_sc_api, columns=['doi', 'load date', 'url', 'title', 'creator', 'publication name'
                                    'volume', 'cover date', 'starting page', 'pii', 'ending page', 'author'])
    
    # Resetando index
    df_sc_api.set_index('doi', inplace=True)
    df_sc_api.reset_index(inplace=True)
    
    return df_sc_api


#######################################################################################


#Função para leitura e padronização dos dados da api do ieee 
def api_ieee(df_ieee_api: pd.DataFrame) -> pd.DataFrame:
    
    #Trantando coluna de autores que estava em formato de dicionario, com vários atores, foi realizado o
    #apply e explode para trazer os dados contidos no interior dos dicionarios, tornando eles em colunas.
    df_ieee_api = pd.concat([df_ieee_api.drop(['authors'], axis=1), 
                             df_ieee_api['authors'].apply(pd.Series)], axis=1)

    df_ieee_api = df_ieee_api.explode('authors')

    df_ieee_api = pd.concat([df_ieee_api.drop(['authors'], axis=1), 
                             df_ieee_api['authors'].apply(pd.Series)], axis=1)

    #Resetando index
    df_ieee_api.set_index('doi', inplace=True)
    df_ieee_api.reset_index(inplace=True)

    #Removendo '-' da coluna issn
    df_ieee_api['issn'].replace('-', '', regex=True, inplace=True)


    #Selecionando as colunas para evitar possiveis retornos inesperados da api
    df_ieee_api = pd.DataFrame(df_ieee_api, columns=['doi', 'title','publisher','isbn','rank','content_type',
                    'abstract','article_number','pdf_url','html_url','abstract_url','publication_title','conference_location'
                    ,'conference_dates','publication_number','is_number','publication_year','publication_date'     
                    ,'start_page','end_page','citing_paper_count','citing_patent_count'  
                    ,'issn','issue','volume','affiliation','authorUrl','id'            
                    ,'full_name','author_order'])         

    #Renomeando coluna
    df_ieee_api.rename(columns={'publication_year': 'year', 'id':'id_author', 'full_name': 'author'}, inplace=True)
    
    return df_ieee_api


#######################################################################################


#Função realiza o join e tratamento dos dados pós o join
def join_csv(df_jcs: pd.DataFrame, df_scimago: pd.DataFrame) -> pd.DataFrame:
    
    #Realizando join entre o arquivo SCIMAGO e JCS
    df_join = pd.merge(df_scimago, df_jcs, on=["id_title"], how="outer")

    #Renomeando coluna
    df_join.rename(columns={'title_x':'title'}, inplace=True)

    #Unificando a coluna title
    df_join = useful.unification('title', df_join, 2)

    #Dropando as colunas title que não serão mais uteis
    df_join.drop(["title_y", "id_title"], axis=1, inplace=True)        
    
    return df_join


#######################################################################################


def union_files(df_acm: pd.DataFrame, df_ieee: pd.DataFrame, df_ScienceDirect: pd.DataFrame, 
                df_api_sc: pd.DataFrame, df_api_ieee: pd.DataFrame, df_csv: pd.DataFrame) -> pd.DataFrame:
    
    #Concatenando os dataframes acm, iee e ScienceDirect
    df_bib = pd.concat([df_acm, df_ieee, df_ScienceDirect], ignore_index=True)

    #Efetuando o join com a coluna issn
    df_end = pd.merge(df_csv, df_bib, on=["issn"], how="right")

    #Concatenando com os dados da api do IEEE e ScienceDirect
    df_end = pd.concat([df_end, df_api_ieee, df_api_sc])
    
    #Renomeando coluna
    df_end.rename(columns={'title':'title_z', 'title_x':'title', 'type_publication_x':'type_publication',
                            'publisher':'publisher_z', 'publisher_x':'publisher'}, inplace=True)

    #Unificando a coluna title, type_publication e publisher
    columns = ['title', 'title_y', 'type_publication', 'publisher', 'publisher_y', 'issn_full']
    df_end = useful.alter_null(columns, df_end)

    df_end = useful.unification('title', df_end, 3)
    df_end = useful.unification('type_publication', df_end, 2)
    df_end = useful.unification('publisher', df_end, 3)

    #Unificando a coluna issn e issn_full (criada na leitura de um dos arquivos csv)
    df_end['issn'] = np.where(df_end['issn_full'] != ' ', df_end['issn_full'], df_end['issn'])

    #Criando coluna hash baseado na coluna title e issn para utilizar no drop duplicates
    df_end = useful.make_str_key(df_end, 'title')
    df_end = useful.make_str_key(df_end, 'issn')

    #Removendo as linhas duplicadas
    df_end.drop_duplicates(subset=['id_title', 'id_issn'], keep='first',inplace=True)

    #Dropando as colunas title que não serão mais uteis
    df_end.drop(["title_y", "title_z", "type_publication_y", "publisher_y", "publisher_z", 
                   "issn_full", "id_issn","id_title"], axis=1, inplace=True)
    
    return df_end