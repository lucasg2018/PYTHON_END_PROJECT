import pandas as pd


def dataframe_filter(df: pd.DataFrame, year: str, title: str) -> pd.DataFrame:
    
    #Realizando o filtro no dataframe baseado nas variáveis que estão preenchidas
    if year != '' and title != '':
        title = title.upper()
        df_filter = df[(df['year'] == year) & (df['title'].str.upper().str.contains(title))]

    elif year != '':
        df_filter = df[df['year'] == year]

    elif title != '':
        title = title.upper()
        df_filter = df[df['title'].str.upper().str.contains(title)]

    else:
        df_filter = df
    
    #Resetando index
    df_filter.set_index('series', inplace=True)
    df_filter.reset_index(inplace=True)
    
    return df_filter