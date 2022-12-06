import pandas as pd
import numpy as np
import hashlib


#Função responsável por gerar o hash
def hash(value):
    return int(hashlib.sha256(value.encode('utf-8')).hexdigest(), 16) % 10**8

#Função responsável por tratar o valor da coluna onde é alfanumérica, 
# removendo também os caracteres especiais antes de jogar na função de hash
def str_key(value):
    if value != ' ': 
        return hash(''.join(str(world).lower() for world in str(value) if str(world).isalnum()))
    return np.nan

#Função responsável por tratar a coluna nula e criando a coluna com sufixo _key,
# já passando as informações para a função str_key
def make_str_key(df: pd.DataFrame, column: str)-> pd.DataFrame: 
    df[column] = df[column].fillna(value=' ')

    column_key = "id_" + column 
    df[column_key] = df[column].apply(str_key) 
                                                
    return df


#######################################################################################


#Função responsável por realizar um case para unificar os dados de 
# colunas com nomes duplicados no momentos do join
def unification(column: str, df: pd.DataFrame,  case: int) -> pd.DataFrame:
    column_y = column + '_y'
    column_z = column + '_z'
    
    if case == 2:
        column_y = column + '_y'
        df[column] = np.where(df[column] == ' ', df[column_y], df[column])
    
    elif case == 3:
        df[column] = np.where(df[column_y] != ' ', df[column_y],
                      np.where(df[column] != ' ', df[column], 
                               df[column_z]))
    
    return df


#######################################################################################


#Função responsável por realizar a alteração em uma lista de colunas que forem nulas para ' '
def alter_null(columns: list, df: pd.DataFrame) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column].fillna(' ')
    
    return df


#######################################################################################


#Função responsável por realizar a alteração em uma lista de colunas que forem nulas para ' '
def hex_letter(word: str) -> str:
    
    hex_word = ''
    for letter in word:
        if not letter.isalnum():
            hex_letter = format(ord(letter), "x")
            hex_word += '%' + str(hex_letter)
        else:
            hex_word += letter
        
    return hex_word