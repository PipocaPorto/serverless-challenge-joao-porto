# coding=utf-8

import urllib.parse # Foi utilizada a biblioteca urllib para coletar informações necessárias para a função
import boto3
import json # Foi utilizada a biblioteca json para melhorar as respostas das funções

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client('s3') # Instância global do s3
table = dynamodb.Table('serverless-challenge-dev') # Instância global da tabela do DynamoDB

# Função para extrair os metadados de uma imagem e salvá-los em uma tabela do DynamoDB
# A entrada desta função é gerada por um evento, no caso é a adição de um novo arquivo no bucket bucket-jpportocampos do s3
def extractMetadata(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name'] # Através do disparo do evento (declarado em severless.yml) extraímos o nome do Bucket do s3

    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') # Através do disparo do evento utilizando a biblioteca urllib extraímos a chave do objeto do s3

    # Try-Except utilizado para garantir maior clareza de resposta em caso de erro na função
    try:
        response = s3.get_object(Bucket = bucket, Key = key) # Função do boto3 get_object retorna o objeto e este é guardado na variável 'response'

        tamanho = response['ContentLength'] # O ContentLength traz um valor decimal da quantidade de bytes no arquivo do objeto, este metadado é salvo na variável 'tamamnho'
        tipo = response['ContentType'] # O ContentType traz o tipo do arquivo do objeto, este metadado é salvo na variável 'tipo'
        hora_upload = str(response['LastModified']) # O LastModified traz um DateTime da última modificação no objeto, como o evento disparado é o upload do arquivo, este metadado é salvo na variável 'hora_upload'
                                                    # O DateTime é transformado em string para melhor visualização dos dados

        # A função do boto3 put_item é usada para adicionar um novo item na tabela do DynamoDB
        table.put_item(
            # O item é criado com as seguintes informações
            Item={
                    's3objectkey': key, # O s3objectkey é guardado utilizando a key estraída no início da função
                    'tamanho': tamanho, # O atributo 'tamanho' é criado e nele armazenamos o 'tamanho' criado
                    'tipo': tipo, # O atributo 'tipo' é criado e nele armazenamos o 'tipo' criado
                    'hora_upload': hora_upload, # O atributo 'hora_upload' é criado e nele armazenamos o 'hora_upload' criado
                }
        )

    # A saída desta função é a adição do item na tabela do DynamoDB

    # Tratamento de exceção caso haja algum erro ao buscar o bucket ou o objeto nele
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

# Função para retornar os metadados da tabela 'serverless-challenge-dev' através de um s3objectkey recebido
# A entrada desta função é gerada por um evento, no caso é a chamda de uma url criada pelo serverless com o path images/{s3objectkey}
# Neste caso específico, o parâmetro do path é o s3objectkey que, devido a como o bucket foi criado, sempre é 'uploads/<nome do arquivo>.<extensão do arquivo>'
# Por isso, é necessário passar 'uploads%2f<nome do arquivo>.<extensão do arquivo>' como parâmetro para que o caractere '/' não seja confundido como um outro path
def getMetadata(event, context):
    key = urllib.parse.unquote_plus(event['pathParameters']['s3objectkey'], encoding='utf-8') # Através do disparo do evento (declarado em severless.yml) utilizando a biblioteca urllib extraímos a chave do objeto do s3
    
     # A função do boto3 get_item é usada para recuperar um item na tabela do DynamoDB
    response = table.get_item(
        TableName = 'serverless-challenge-dev', # É passada a tabela na qual se quer recuperar o item
        Key = {
            's3objectkey': key # É passada a chave do item a ser recuperado
        }
    )
    
    response['Item']['tamanho'] = float(response['Item']['tamanho']) # Para que a resposta seja gerada sem erros, o 'tamanho' do objeto resgatado precisa ser transformado em float

    # Como a função get_item retorna outros metadados criados pelo DynamoDB, é retornado apenas o metadado 'Item', que é commposto pelos, no caso, metadados salvos da imagem
    # A variável 'response' será retornada com os detalhes criados         
    response = { 'body': json.dumps(response['Item']), # É criado um body usando a blibioteca json que possui o 'Item' da response
                     'statusCode': 200 } # É criado um status code 200 para indicar o sucesso
    
    # A saída desta função é a variável response
    return response

# Função que faz o download de um arquivo do s3 através de um s3objectkey recebido
# Como a função é uma função lambda, o download é realizado no aramzenamento do AWS Lambda
# A entrada desta função é gerada por um evento, no caso é a chamda de uma url criada pelo serverless com o path images/download/{s3objectkey}
# Neste caso específico, o parâmetro do path é o s3objectkey que, devido a como o bucket foi criado, sempre é 'uploads/<nome do arquivo>.<extensão do arquivo>'
# Por isso, é necessário passar 'uploads%2f<nome do arquivo>.<extensão do arquivo>' como parâmetro para que o caractere '/' não seja confundido como um outro path
def getImage(event, context):
    bucket = 'bucket-jpportocampos' # Como o evento não está atrelado ao s3, o bucket desejado é armazenado em uma variável 'bucket'

    key = urllib.parse.unquote_plus(event['pathParameters']['s3objectkey'], encoding='utf-8') # Através do disparo do evento (declarado em severless.yml) utilizando a biblioteca urllib extraímos a chave do objeto do s3
    
    # Try-Except utilizado para garantir maior clareza de resposta em caso de erro na função
    try:
        s3.download_file(bucket, key, '/tmp/imagem-desafio.jpg') # Função do boto3 download_file recebe o bucket desejado, o s3objectkey e o caminho para download do arquivo, o /tmp/ é obrigatório para a função por ser uma função lambda

        # É criada uma variável 'message' que armazena uma mensagem de sucesso para ser apresentada na saída da função
        message = {
            'message' : 'Download successful!'
        }

        # É criada uma variável 'response' que será retornada com os detalhes criados
        response = { 'body': json.dumps(message), # É criado um body usando a blibioteca json que dispara a mensagem de sucesso em 'message'
                     'statusCode': 200 } # É criado um status code 200 para indicar o sucesso
        
        # A saída desta função é a variável response
        return response

    # Tratamento de exceção caso haja algum erro ao buscar o bucket ou o objeto nele
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

# Função que retorna algumas informações sobre os dados na tabela do DynamoDB
# A entrada desta função é gerada por um evento, no caso é a chamda de uma url criada pelo serverless com o path images/info
def InfoImages(event, context):
     # Try-Except utilizado para garantir maior clareza de resposta em caso de erro na função
    try:
        response = table.scan() # A função scan do boto3 retorna todos os itens da tabela do DynamoDB

        data = response['Items'] # Como a função scan retorna outros metadados criados pelo DynamoDB, é criado o array 'data', que é commposto pelos atributos de cada um dos itens da tabela ('Items')

        tamanho_inicial_maior = 0 # É criada uma variável 'tamanho_inicial_maior' que será utilizada para definir o item com maior tamanho, o valor dela é 0 pois nenhum item terá menos do que 1 byte
        maior_tamanho = None # É criada uma variável 'maior_tamanho' que será utilizada para guardar o item com maior tamanho

        tamanho_inicial_menor = 9999999 # É criada uma variável 'tamanho_inicial_menor' que será utilizada para definir o item com menor tamanho, o valor 9999999 é usado pois é um valor que ultrapassa o limite de bytes que podem ser salvos em um atributo
        menor_tamanho = None # É criada uma variável 'menor_tamanho' que será utilizada para guardar o item com menor tamanho

        tipos = {} # É criado um dictionary vazio que irá armazenar os tipos e a quantidade de imagens de cada tipo

        # Todos os dados são processados de uma vez em um for loop para diminuir a complexidade da função
        for item in data:
            if float(item['tamanho']) >= tamanho_inicial_maior: # O tamanho salvo em decimal é convertido para float e comparado (maior ou igual a) com a variável 'tamanho_inicial_maior'
                tamanho_inicial_maior = float(item['tamanho']) # Caso o tamanho seja maior ou igual, ele é salvo na variável 'tamanho_inicial_maior' em formato float
                maior_tamanho = item # O item é guardado na variável 'maior_tamanho'
            
            if float(item['tamanho']) < tamanho_inicial_menor: # O tamanho salvo em decimal é convertido para float e comparado (menor que) com a variável 'tamanho_inicial_menor'
                tamanho_inicial_menor = float(item['tamanho']) # Caso o tamanho seja menor, ele é salvo na variável 'tamanho_inicial_menor' em formato float
                menor_tamanho = item # O item é guardado na variável 'menor_tamanho'

            if item['tipo'] in tipos: # Se o tipo do item já existir no dictionary 'tipos'
                tipos[item['tipo']] = tipos[item['tipo']] + 1 # É somado 1 no item 'tipo' no dictionary 'tipos' (caso o item não exista ele é criado)
            else:
                tipos[item['tipo']] = 1 # Se não é criado um novo item no dictionary com o tipo e valor 1

        # É criado o dictionary info com as informações a serem retornadas na função
        info = {
            'maior_imagem': maior_tamanho['s3objectkey'], # O s3objectkey da imagem de maior tamanho é salvo em 'maior_imagem'
            'menor_imagem': menor_tamanho['s3objectkey'], # O s3objectkey da imagem de menor tamanho é salvo em 'menor_imagem'
            'tipos_imagem': list(tipos.keys()), # Os tipos salvos nos itens do dictionary 'tipos' são salvos em uma lista
            'quantidades_tipos': tipos # Os itens do dictionary 'tipos' são salvos
        }

        # É criada uma variável 'response' que será retornada com os detalhes criados
        response = { 'body': json.dumps(info), # É criado um body usando a blibioteca json que possui o dictionary 'info'
                     'statusCode': 200 } # É criado um status code 200 para indicar o sucesso

        # A saída desta função é a variável response
        return response

    # Tratamento de exceção caso haja algum erro ao buscar a tabela
    except Exception as e:
        print(e)
        print('Error getting table {}. Make sure it exist and is in the same region as this function.'.format(table))
        raise e