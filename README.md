# PYTHON_END_PROJECT

## Passos para execução

1.  Crie o ambiente virtual, abra o terminal dentro da pasta do projeto e digite o comandoa seguir:
    - python -m venv nome_do_ambiente_virtual
 
2.  Instalando no terminal as bibliotecas utilizadas:
    - pip install fastapi
    - pip install uvicorn
    - pip install uuid
    - pip install pandas
    - pip install bibtexparser
    - pip install requests
    - pip install numpy
    - pip install sqlalchemy
    - pip install urllib
    - pip install hashlib
    
3.  Após realizar os passos podemos executar nosso servidor pelo terminal com o comando:
    - uvicorn main:app --reload
    
4.  Podemos visualizar e interagir com a documentação gerada automaticamente pelo Swagger UI, permitindo que realizar o teste de execução da api, basta acrescentar a url **/docs** e realizar o teste com a documentação interativa.
    - ![This is an image](https://www.alura.com.br/artigos/assets/como-criar-apis-python-usando-fastapi/imagem5.png)
