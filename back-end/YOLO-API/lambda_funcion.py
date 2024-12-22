import json
import uuid
import requests
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
dynamodb_table = dynamodb.Table('yolo')

status_check_path = '/status'
cliente_path = '/cliente'
clientes_path = '/clientes'
dataBase_load = '/load_external_data'
external_data_endpoint = 'https://3ji5haxzr9.execute-api.us-east-1.amazonaws.com/dev/caseYolo'

def lambda_handler(event, context):
    print('Request event: ', event)
    response = None

    try:
        http_method = event.get('httpMethod')
        path = event.get('path')

        if http_method == 'GET' and path == status_check_path:
            response = build_response(200, 'serviço está funcionando')
        elif http_method == 'GET' and path == dataBase_load:
            # Chamada HTTP GET para o endpoint externo
            external_response = requests.get(external_data_endpoint)

            if external_response.status_code == 200:
                # Parse o conteúdo JSON e manipule
                external_data = external_response.json()

                try:
                    external_body = json.loads(external_data.get('body'))  # Decodifica para objeto Python
                except json.JSONDecodeError:
                    external_body = external_data.get('body')  # Se falhar, usa o corpo como string sem modificar

                clientes = external_body.get("clientes", [])  # Assume que os clientes estão sob a chave "clientes"
                if not isinstance(clientes, list):
                    return build_response(400, 'Formato inválido para a lista de clientes.')

                cadastrados = 0  # Contador de clientes cadastrados com sucesso

                for cliente in clientes:
                    try:
                        # Gera um ID único para cada cliente
                        cliente['clientesId'] = str(uuid.uuid4())
                        # Salva o cliente no DynamoDB
                        dynamodb_table.put_item(Item=cliente)
                        cadastrados += 1
                    except ClientError as e:
                        print(f"Erro ao salvar cliente: {e}")

                # Corpo da resposta com o número de clientes cadastrados
                response_body = {
                    'mensagem': 'Clientes cadastrados com sucesso.',
                    'clientesCadastrados': cadastrados
                }
            else:
                # Caso o serviço externo não retorne 200 OK
                response_body = f'Erro ao buscar dados no endpoint externo: {external_response.status_code}'

            response = build_response(200, response_body)

        elif http_method == 'GET' and path == cliente_path:
            cliente_id = event['queryStringParameters']['clientesId']
            response = get_cliente(cliente_id)
        elif http_method == 'GET' and path == clientes_path:
            response = get_clientes()
        elif http_method == 'POST' and path == clientes_path:
            response = save_cliente(json.loads(event['body']))
        elif http_method == 'PATCH' and path == clientes_path:
            body = json.loads(event['body'])
            response = modify_cliente(body['clientesId'], body['updateKey'], body['updateValue'])
        elif http_method == 'DELETE' and path == clientes_path:
            body = json.loads(event['body'])
            response = delete_cliente(body['clientesId'])
        else:
            response = build_response(404, '404 Not Found')

    except Exception as e:
        print('Error:', e)
        response = build_response(400, 'Error processing request')

    return response

def get_cliente(cliente_id):
    try:
        response = dynamodb_table.get_item(Key={'clientesId': cliente_id})
        return build_response(200, response.get('Item'))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def get_clientes():
    try:
        scan_params = {
            'TableName': dynamodb_table.name
        }
        return build_response(200, scan_dynamo_records(scan_params, []))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def scan_dynamo_records(scan_params, item_array):
    response = dynamodb_table.scan(**scan_params)
    item_array.extend(response.get('Items', []))

    if 'LastEvaluatedKey' in response:
        scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        return scan_dynamo_records(scan_params, item_array)
    else:
        return {'clientes': item_array}

def save_cliente(request_body):
    try:
        # Gera um novo clienteId
        cliente_id = str(uuid.uuid4())  # Gera um UUID único
        request_body['clientesId'] = cliente_id  # Adiciona o clienteId ao item

        dynamodb_table.put_item(Item=request_body)

        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': request_body
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def modify_cliente(cliente_id, update_key, update_value):
    try:
        response = dynamodb_table.update_item(
            Key={'clientesId': cliente_id},
            UpdateExpression=f'SET {update_key} = :value',
            ExpressionAttributeValues={':value': update_value},
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdatedAttributes': response
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def delete_cliente(cliente_id):
    try:
        response = dynamodb_table.delete_item(
            Key={'clientesId': cliente_id}
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'Item': response
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        return super(DecimalEncoder, self).default(obj)

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body, cls=DecimalEncoder, ensure_ascii=False)
    }
