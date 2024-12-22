
# Trarefas
1. [x] Criar o ambiente no GitHub para o hospedagem dos códigos e arquivos
2. [x] Criar um ambiente na AWS usando a arquitetura de microsserviços
3. [x] Criar uma base de dados simples para o cadastro de pessoas no AWS em Dynamo DB, ou RDS
4. [x] Fazer um integração com a nossa API REST para importar os dados dos pessoas para a base criada.*
5. [x] Criar uma aplicação para listar e cadastrar pessoas, filtrar por tipo nas linguagens escolhidas (back-end e front-end)*
6. [x] Documentar os métodos e funções criadas


# Documentação da API - YOLO

Esta documentação descreve as rotas e métodos disponíveis na API para gerenciar os clientes.

documentação da api 

## link de acesso para o projeto 

https://master.d2uz4858i8z92y.amplifyapp.com/


## seriviços utilizados da Aws
- Lambda
- dy DB
- Amplify
- API Gateway

## Importar clientes 


## deletar :

url:https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes

{
    "clientesId": "042bf372-13c7-4354-a832-818b3215e51d"
}

## post 

url: https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes


{"Nome": "Fiona", 
"Telefone": "+55 73 93305-2891", 
"E-mail": "fiona@example.com", 
"Tipo": "Fornecedor",
"Data de Cadastro": "2024-08-01"}

## get all

url: https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes

## get by Id:
url: https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/cliente
params: 
key:clientesId
vlaue: 
id: xxxxxx-xxxx
 
https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/cliente?clientesId=28f187c3-d69f-483a-9301-d635968b254a


## update:
##  PATCH
url:https://6xjykyia4l.execute-api.us-east-2.amazonaws.com/dev/clientes
ex:{
  "clientesId": "079d6087-48e6-4f21-8cc1-58ecc8de6703",
  "updateKey": "Nome",
  "updateValue": "Jane Cleide"
}




