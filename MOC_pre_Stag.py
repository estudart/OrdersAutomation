import pandas as pd
# from tkinter import ttk, Label
# import tkinter as tk
import time
import json
import requests
import websocket
import msgpack
# import threading
from multiprocessing import Process, freeze_support

with open("P:/Python ETS/MOCK_pre_abertura/credentials_Prod.txt", "r") as arq:
    texto = arq.read()

trata = texto.split('"')

client_ID = str(trata[1])
client_Secret = str(trata[3])

BASE_URL = 'https://mtbserver.americastg.com.br:51511/api'
BASE_URL_WS = 'https://mtbserver.americastg.com.br:51546/api/ws'
TOKEN_URL = 'https://mtbserver.americastg.com.br:51525/connect/token'


plan = pd.read_excel('P:/Python ETS/MOCK_pre_abertura/Ordens.xlsx')


# Request do token
token_request = {
    'grant_type': 'client_credentials',  # do not change
    'scope': 'atgapi',    # do not change
    'client_id': client_ID,
    'client_secret': client_Secret
}

updateFreq = 1  # integer, if not set, defaults to 500ms


def get_token():
    response = requests.post(TOKEN_URL, data=token_request)
    response.raise_for_status()
    return response.json()['access_token']


class BaseClient:
    def __init__(self, headers, endpoint):
        self.headers = headers
        self.endpoint = endpoint

    def get(self, page):
        response = requests.get(
            f'{BASE_URL}/{self.endpoint}?page={page}', headers=self.headers)
        response.raise_for_status()
        return response

    def get_by_id(self, strategy_id):
        response = requests.get(
            f'{BASE_URL}/{self.endpoint}/{strategy_id}', headers=self.headers)
        response.raise_for_status()
        return response

    def is_order_updatable(self, status):
        return status != 'CANCELLED' and status != 'FINISHED' and status != 'TOTALLY_EXECUTED'

    def new(self, new_request):
        response = requests.post(f'{BASE_URL}/{self.endpoint}', data=json.dumps(
            new_request), headers=self.headers)
        response.raise_for_status()
        return response

    def update(self, update_request, strategy_id):
        response = requests.put(
            f'{BASE_URL}/{self.endpoint}/{strategy_id}', data=json.dumps(update_request), headers=self.headers)
        response.raise_for_status()
        return response

    def cancel(self, strategy_id):
        response = requests.delete(
            f'{BASE_URL}/{self.endpoint}/{strategy_id}', headers=self.headers)
        response.raise_for_status()
        return response


def on_error(ws, error):
    print("Error:")
    print(error)


# 1) Update para modificar o valor dentro do preço teórico
token = get_token()

BROKER = str(int(plan.loc[0, 'broker_teste']))
ACCOUNT = str(int(plan.loc[0, 'conta_teste']))
print(BROKER, ACCOUNT)


def envia_basket():
    global dict_symbols
    for orders in range(len(planilha['conta'].tolist())):
        id_corretora = str(int(planilha.loc[orders, 'id_corretora']))
        conta = str(int(planilha.loc[orders, 'conta']))
        symbol = str(planilha.loc[orders, 'symbol'])
        side = str(planilha.loc[orders, 'side'])
        quantity = int(planilha.loc[orders, 'quantity'])
        time_in_force = str(planilha.loc[orders, 'time_in_force'])

        new_request = {
            'Broker': id_corretora,
            'Account': conta,
            'OrderType': 'MARKET_LIMIT',
            'Symbol': symbol,
            'Side': side,
            'Quantity': quantity,
            'TimeInForce': time_in_force,
        }

        access_token = get_token()
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + access_token
        }

        print(new_request)
        client = BaseClient(headers, 'simple-order')
        client.new(new_request)


def envia_basket_leilao():
    global dict_symbols
    for orders in range(len(planilha['conta'].tolist())):
        id_corretora = str(planilha.loc[orders, 'id_corretora'])
        conta = str(planilha.loc[orders, 'conta'])
        symbol = str(planilha.loc[orders, 'symbol'])
        side = str(planilha.loc[orders, 'side'])
        quantity = int(planilha.loc[orders, 'quantity'])
        mercado = str(planilha.loc[orders, 'mercado'])
        price = float(planilha.loc[orders, 'price'])

        if mercado == 'BOV':
            new_request = {
                'Broker': id_corretora,
                'Account': conta,
                'Symbol': symbol,
                'Side': side,
                'Quantity': quantity,
                'TimeInForce': 'MOA'
            }
        else:
            new_request = {
                'Broker': id_corretora,
                'Account': conta,
                'Symbol': symbol,
                'Side': side,
                'Quantity': quantity,
                'TimeInForce': 'DAY',
                'Price': price
            }

        access_token = get_token()
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + access_token
        }

        print(f"new_request: {new_request}")
        client = BaseClient(headers, 'simple-order')
        client.new(new_request)


def algo_leilao(update_request_preco_menor,
                update_request_preco_maior,
                update_request_diminuindo,
                update_request_aumentando,
                new_request):

    cenary = []

    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    print()

    strategy_id = response.json()['StrategyId']

    time.sleep(1)

    # print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    status = response.json()['Status']
    print(response.text)
    print()

    if client.is_order_updatable(status):
        # print('*** UPDATING SIMPLE ORDER ***')
        response = client.update(update_request_preco_menor, strategy_id)
        print(response.text)
        response = json.loads(response.text)

        if response["Success"] is not True:
            cenary.append('teste OK')
        else:
            cenary.append('teste falhou')

        time.sleep(1)

        if client.is_order_updatable(status):
            # print('*** UPDATING SIMPLE ORDER ***')
            response = client.update(update_request_preco_maior, strategy_id)
            print(response.text)
            response = json.loads(response.text)

            if response["Success"] is True:
                cenary.append('teste OK')
            else:
                cenary.append('teste falhou')

            time.sleep(1)

            if client.is_order_updatable(status):
                # print('*** UPDATING SIMPLE ORDER ***')
                response = client.update(update_request_diminuindo, strategy_id)
                print(response.text)
                response = json.loads(response.text)

                if response["Success"] is not True:
                    cenary.append('teste OK')
                else:
                    cenary.append('teste falhou')

                time.sleep(1)

                if client.is_order_updatable(status):
                    # print('*** UPDATING SIMPLE ORDER ***')
                    response = client.update(update_request_aumentando, strategy_id)
                    print(response.text)
                    response = json.loads(response.text)

                    if response["Success"] is True:
                        cenary.append('teste OK')
                    else:
                        cenary.append('teste falhou')
                    return cenary


def algo_leilao_bmf(update_request_preco_menor,
                    update_request_diminuindo,
                    new_request):

    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    print()

    strategy_id = response.json()['StrategyId']

    # print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    status = response.json()['Status']
    print(response.text)
    print()

    time.sleep(1)

    if client.is_order_updatable(status):
        # print('*** UPDATING SIMPLE ORDER ***')
        response = client.update(update_request_preco_menor, strategy_id)
        print(response.text)
        response = json.loads(response.text)

        if response["Success"] is not True:
            cenary.append('teste OK')
        else:
            cenary.append('teste falhou')

        time.sleep(1)

        if client.is_order_updatable(status):
            # print('*** UPDATING SIMPLE ORDER ***')
            response = client.update(update_request_diminuindo, strategy_id)
            print(response.text)
            response = json.loads(response.text)

            if response["Success"] is not True:
                cenary.append('teste OK')
            else:
                cenary.append('teste falhou')
    return cenary


def algo_fora_leilao_bmf(update_request_preco_menor,
                         update_request_diminuindo,
                         new_request):

    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    print()

    strategy_id = response.json()['StrategyId']

    time.sleep(1)

    print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    status = response.json()['Status']
    print(response.text)
    print()

    time.sleep(1)

    if client.is_order_updatable(status):
        print('*** UPDATING SIMPLE ORDER ***')
        response = client.update(update_request_preco_menor, strategy_id)
        print(response.text)
        response = json.loads(response.text)

        if response["Success"] is True:
            cenary.append('teste OK')
        else:
            cenary.append('teste falhou')

        time.sleep(1)

        if client.is_order_updatable(status):
            print('*** UPDATING SIMPLE ORDER ***')
            response = client.update(update_request_diminuindo, strategy_id)
            print(response.text)
            response = json.loads(response.text)

            if response["Success"] is True:
                cenary.append('teste OK')
            else:
                cenary.append('teste falhou')
        return cenary


def cancela_leilao(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)

    strategy_id = response.json()['StrategyId']

    time.sleep(1)

    # print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    print(response.text)

    # print('*** CANCELLING SIMPLE ORDER ***')
    response = client.cancel(strategy_id)
    print(response.text)
    response = json.loads(response.text)

    time.sleep(1)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def cancela_leilao_after(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)

    strategy_id = response.json()['StrategyId']

    time.sleep(1)

    # print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    print(response.text)

    # print('*** CANCELLING SIMPLE ORDER ***')
    response = client.cancel(strategy_id)
    print(response.text)
    response = json.loads(response.text)

    time.sleep(1)

    if response["Success"] is not True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def cancela_market_bmf(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)

    strategy_id = response.json()['StrategyId']

    # print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    print(response.text)

    # print('*** CANCELLING SIMPLE ORDER ***')
    response = client.cancel(strategy_id)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def envio_stop_neg_bmf(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def bmf_neg_piora_preco_quantidade(new_request,
                                   update_request_diminuindo,
                                   update_request_preco_menor):

    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    print()

    strategy_id = response.json()['StrategyId']

    time.sleep(1)

    print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    status = response.json()['Status']
    print(response.text)
    print()

    time.sleep(1)

    if client.is_order_updatable(status):
        print('*** UPDATING SIMPLE ORDER ***')
        response = client.update(update_request_diminuindo, strategy_id)
        print(response.text)
        response = json.loads(response.text)

        if response["Success"] is True:
            cenary.append('teste OK')
        else:
            cenary.append('teste falhou')

        time.sleep(1)

        if client.is_order_updatable(status):
            print('*** UPDATING SIMPLE ORDER ***')
            response = client.update(update_request_preco_menor, strategy_id)
            print(response.text)
            response = json.loads(response.text)

            if response["Success"] is True:
                cenary.append('teste OK')
            else:
                cenary.append('teste falhou')
        return cenary


def cancela_fora_leilao(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)

    strategy_id = response.json()['StrategyId']

    # print('*** GETTING SIMPLE ORDER BY ID ***')
    response = client.get_by_id(strategy_id)
    print(response.text)

    time.sleep(1)

    # print('*** CANCELLING SIMPLE ORDER ***')
    response = client.cancel(strategy_id)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def moa_leilao(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def envio_stop(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is not True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def envio_stop_market(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def envio_ioc(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def envio_fok(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    # print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is not True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def envio_acima_banda_bov(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is not True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


def moc_leilao(new_request):
    cenary = []
    access_token = get_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }

    client = BaseClient(headers, 'simple-order')

    time.sleep(1)

    #print('*** CREATING SIMPLE ORDER ***')
    response = client.new(new_request)
    print(response.text)
    response = json.loads(response.text)

    if response["Success"] is True:
        cenary.append('teste OK')
    else:
        cenary.append('teste falhou')
    return cenary


##################################################################################################


def processmsg_bov(symbol_bov, preco_leilao_bov):

    try:

        new_request_bov = {
                'Broker': BROKER,
                'Account': ACCOUNT,
                'OrderType': 'LIMIT',
                'Symbol': symbol_bov,
                'Side': 'BUY',
                'Quantity': 200,
                'TimeInForce': 'DAY',
                'Price': preco_leilao_bov
            }

        new_request_moa_bov = {
                'Broker': BROKER,
                'Account': ACCOUNT,
                'Symbol': symbol_bov,
                'Side': 'BUY',
                'Quantity': 200,
                'TimeInForce': 'MOA',
                'Price': preco_leilao_bov
            }

        update_request_preco_menor_bov = {
                'Price': int(preco_leilao_bov * 0.97)
            }

        update_request_preco_maior_bov = {
                'Price': int(preco_leilao_bov * 1.07)
            }

        update_request_diminuindo_bov = {
                'Quantity': 100
            }

        update_request_aumentando_bov = {
                'Quantity': 300
            }

        teste_2 = algo_leilao(update_request_preco_menor_bov,
                              update_request_preco_maior_bov,
                              update_request_diminuindo_bov,
                              update_request_aumentando_bov,
                              new_request_bov)

        teste_3_bov = cancela_leilao(new_request_bov)

        teste_4 = moa_leilao(new_request_moa_bov)

        print(f'\n\nTESTE BOV {symbol_bov}\n'
              f'BOV leilão modificação de uma oferta dentro do preço teórico, reduzindo a quantidade:'
              f'{teste_2[0]} / {teste_2[2]}'
              f'\nCancelamento de uma oferta de compra dentro do preço teórico (antes):'
              f'{teste_3_bov[0]}'
              f'\nEnvio de oferta a mercado (MOA) para um ativo no mercado a vista:'
              f'{teste_4[0]}'
              f'\nCancelamento de uma oferta de compra dentro do preço teórico: ')

        with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_pre_BOV.txt", "w") as arquivo:
            arquivo.write(f'****** Fase pré-abertura ******\n\n'
                          f'\n\nTESTE BOV {symbol_bov}\n'
                          f'BOV leilão modificação de uma oferta dentro do preço teórico, reduzindo a quantidade: '
                          f'{teste_2[0]} / {teste_2[2]} '
                          f'\nCancelamento de uma oferta de compra dentro do preço teórico (antes): {teste_3_bov[0]} '
                          f'\nEnvio de oferta a mercado (MOA) para um ativo no mercado a vista: {teste_4[0]}')

        envia_basket_leilao()

        time.sleep(80)
        teste_5_bov = cancela_leilao_after(new_request_bov)
        with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_pre_BOV.txt", "a") as arquivo:
            arquivo.write(f'\nCancelamento de uma oferta de compra dentro do preço teórico: {teste_5_bov[0]} '
                          f'\n\n')

    except:
        print(f"processmsg_bov: {dict_symbols}")


def processmsg_bmf(symbol_bmf, preco_leilao_bmf):

    preco_leilao_bmf_abaixo = int(preco_leilao_bmf * 0.94)
    preco_leilao_bmf_acima = int(preco_leilao_bmf * 1.06)

    while preco_leilao_bmf % 5 != 0:
        preco_leilao_bmf += 1

    while preco_leilao_bmf_abaixo % 5 != 0:
        preco_leilao_bmf_abaixo -= 1

    while preco_leilao_bmf_acima % 5 != 0:
        preco_leilao_bmf_acima += 1

    new_request_bmf = {
            'Broker': BROKER,
            'Account': ACCOUNT,
            'OrderType': 'LIMIT',
            'Symbol': symbol_bmf,
            'Side': 'BUY',
            'Quantity': 500,
            'TimeInForce': 'DAY',
            'Price': preco_leilao_bmf
        }

    new_request_bmf_fora_leilao = {
            'Broker': BROKER,
            'Account': ACCOUNT,
            'OrderType': 'LIMIT',
            'Symbol': symbol_bmf,
            'Side': 'BUY',
            'Quantity': 500,
            'TimeInForce': 'DAY',
            'Price': preco_leilao_bmf_abaixo
        }

    new_request_bmf_fora_leilao2 = {
            'Broker': BROKER,
            'Account': ACCOUNT,
            'OrderType': 'LIMIT',
            'Symbol': symbol_bmf,
            'Side': 'SELL',
            'Quantity': 500,
            'TimeInForce': 'DAY',
            'Price': preco_leilao_bmf_acima
        }

    new_request_bmf_ioc = {
            'Broker': BROKER,
            'Account': ACCOUNT,
            'OrderType': 'LIMIT',
            'Symbol': symbol_bmf,
            'Side': 'SELL',
            'Quantity': 500,
            'TimeInForce': 'IOC',
            'Price': preco_leilao_bmf
        }

    new_request_bmf_fok = {
            'Broker': BROKER,
            'Account': ACCOUNT,
            'OrderType': 'LIMIT',
            'Symbol': symbol_bmf,
            'Side': 'SELL',
            'Quantity': 500,
            'TimeInForce': 'FOK',
            'Price': preco_leilao_bmf
        }

    new_request_bmf_stop = {
            'Broker': BROKER,
            'Account': ACCOUNT,
            'OrderType': 'STOP_LIMIT',
            'Symbol': symbol_bmf,
            'Side': 'BUY',
            'Quantity': 500,
            'TimeInForce': 'DAY',
            'Price': preco_leilao_bmf_acima,
            "StopTriggerPrice": preco_leilao_bmf_acima
        }

    update_request_preco_menor_bmf = {
            'Price': preco_leilao_bmf_abaixo
        }

    update_request_diminuindo_bmf = {
            'Quantity': 100
        }

    teste_1e2_bmf = algo_leilao_bmf(update_request_preco_menor_bmf,
                                    update_request_diminuindo_bmf,
                                    new_request_bmf)

    teste_3e4_bmf = algo_fora_leilao_bmf(update_request_preco_menor_bmf,
                                         update_request_diminuindo_bmf,
                                         new_request_bmf_fora_leilao)

    teste_5_bmf = cancela_leilao(new_request_bmf)

    teste_6_bmf = cancela_fora_leilao(new_request_bmf_fora_leilao2)

    teste_7_bmf = envio_stop(new_request_bmf_stop)

    teste_8_bmf = envio_ioc(new_request_bmf_ioc)

    teste_9_bmf = envio_fok(new_request_bmf_fok)

    print(f'TESTE BMF {symbol_bmf}\n'
          f'BMF leilão modificação de oferta dentro do preço teórico, reduzindo a quantidade: '
          f'{teste_1e2_bmf[0]} \n'
          f'BMF leilão modificação de oferta dentro do preço teórico, piorando o preço: '
          f'{teste_1e2_bmf[1]} \n'
          f'BMF leilão modificação de oferta fora do preço teórico, reduzindo a quantidade:'
          f'{teste_3e4_bmf[0]} \n'
          f'BMF leilão modificação de oferta fora do preço teórico, piorando o preço:'
          f'{teste_3e4_bmf[1]} \n'
          f'Cancelamento de oferta de compra com preço participando do teórico (antes):'
          f'{teste_5_bmf[0]} \n'
          f'Cancelamento de oferta de venda com preço maior do que o teórico:'
          f'{teste_6_bmf[0]} \n'
          f'Envio de oferta STOP para todos o instrumento de BMF: '
          f'{teste_7_bmf[0]} \n'
          f'Envio de oferta EOC / FAK para todos o instrumento de BMF: '
          f'{teste_8_bmf[0]} \n'
          f'Envio de oferta TON / FOK para todos o instrumento de BMF: '
          f'{teste_9_bmf[0]} \n'
          f'\n\n')

    with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_pre_BMF.txt", "w") as arquivo:
        arquivo.write(f'****** Fase pré-abertura ******\n\n'
                      f'TESTE BMF {symbol_bmf}\n'
                      f'BMF leilão modificação de oferta dentro do preço teórico, reduzindo a quantidade: '
                      f'{teste_1e2_bmf[0]} \n'
                      f'BMF leilão modificação de oferta dentro do preço teórico, piorando o preço: '
                      f'{teste_1e2_bmf[1]} \n'
                      f'BMF leilão modificação de oferta fora do preço teórico, reduzindo a quantidade: '
                      f'{teste_3e4_bmf[0]} \n'
                      f'BMF leilão modificação de oferta fora do preço teórico, piorando o preço: '
                      f'{teste_3e4_bmf[1]} \n'
                      f'Cancelamento de oferta de compra com preço participando do teórico: '
                      f'{teste_5_bmf[0]} \n'
                      f'Cancelamento de oferta de venda com preço maior do que o teórico: '
                      f'{teste_6_bmf[0]} \n'
                      f'Envio de oferta STOP para todos o instrumento de BMF: '
                      f'{teste_7_bmf[0]} \n'
                      f'Envio de oferta EOC / FAK para todos o instrumento de BMF: '
                      f'{teste_8_bmf[0]} \n'
                      f'Envio de oferta TON / FOK para todos o instrumento de BMF: '
                      f'{teste_9_bmf[0]} \n')

    time.sleep(61)
    teste_10_bmf = cancela_leilao(new_request_bmf)
    with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_pre_BMF.txt", "a") as arquivo:
        arquivo.write(f'Cancelamento de oferta de compra com preço participando do teórico:'
                      f'{teste_10_bmf[0]} \n'
                      f'\n\n')


def processmsg_market_bmf(symbol_bmf, preco_market_bmf):
    preco_market_bmf_abaixo = int(preco_market_bmf * 0.94)
    preco_market_bmf_acima = int(preco_market_bmf * 1.06)

    if symbol_bmf[:2] == 'DI':
        preco_market_bmf_abaixo = int(preco_market_bmf - (10 * 0.010))
        preco_market_bmf_acima = int(preco_market_bmf + (10 * 0.010))

    if symbol_bmf[:2] in ('DOL', 'WDO'):
        preco_market_bmf_abaixo = int(preco_market_bmf - (3 * 30))
        preco_market_bmf_acima = int(preco_market_bmf + (3 * 30))

    if symbol_bmf[:2] in ('IND', 'WIN'):
        preco_market_bmf_abaixo = int(preco_market_bmf - (3 * 30))
        preco_market_bmf_acima = int(preco_market_bmf + (3 * 30))

    new_request_neg_bmf = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'LIMIT',
        'Symbol': symbol_bmf,
        'Side': 'BUY',
        'Quantity': 120,
        'TimeInForce': 'DAY',
        'Price': preco_market_bmf,
    }

    update_request_preco_menor = {
        'Price': preco_market_bmf_abaixo
    }

    update_request_diminuindo = {
        'Quantity': 100
    }

    teste_1 = bmf_neg_piora_preco_quantidade(new_request_neg_bmf,
                                             update_request_diminuindo,
                                             update_request_preco_menor)

    teste_2 = cancela_market_bmf(new_request_neg_bmf)

    new_request_neg_bmf_stop = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'STOP_LIMIT',
        'Symbol': symbol_bmf,
        'Side': 'BUY',
        'Quantity': 200,
        'TimeInForce': 'DAY',
        'Price': preco_market_bmf_acima,
        'StopTriggerPrice': preco_market_bmf_acima
    }

    teste_3 = envio_stop_neg_bmf(new_request_neg_bmf_stop)

    teste_4 = cancela_market_bmf(new_request_neg_bmf_stop)

    new_request_aparente_neg_bmf = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'LIMIT',
        'Symbol': symbol_bmf,
        'Side': 'BUY',
        'Quantity': 500,
        'TimeInForce': 'DAY',
        'Price': preco_market_bmf,
        'DisplayQuantity': 100
    }

    teste_5 = envio_stop_neg_bmf(new_request_aparente_neg_bmf)

    teste_6 = envio_stop_neg_bmf(new_request_neg_bmf)

    print(f'\n\nNegociação {symbol_bmf}\n'
          f'Modificação de oferta reduzindo a quantidade: {teste_1[0]}\n'
          f'Modificação de oferta piorando o preço: {teste_1[1]}\n'
          f'Cancelamento de oferta sem que o instrumento esteja em leilão: {teste_2[0]}\n'
          f'Envio de oferta STOP de compra: {teste_3[0]}\n'
          f'Cancelamento da oferta STOP de compra: {teste_4[0]}\n'
          f'Envio de oferta de compra de {symbol_bmf} de 500 lotes com quantidade aparente de 100 lotes: {teste_5[0]}\n'
          f'Envio de oferta de compra de {symbol_bmf} com o mesmo preço da oferta com lote aparente: {teste_6[0]}\n'
          f'\n\n')

    with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_BMF_Neg.txt", "w") as arquivo:
        arquivo.write(f'****** Negociação {symbol_bmf} ******\n\n'
                      f'Modificação de oferta reduzindo a quantidade: '
                      f'{teste_1[0]}\n'
                      f'Modificação de oferta piorando o preço: '
                      f'{teste_1[1]}\n'
                      f'Cancelamento de oferta sem que o instrumento esteja em leilão: '
                      f'{teste_2[0]}\n'
                      f'Envio de oferta STOP de compra: '
                      f'{teste_3[0]}\n'
                      f'Cancelamento da oferta STOP de compra: '
                      f'{teste_4[0]}\n'
                      f'Envio de oferta de compra de {symbol_bmf} de 500 lotes com quantidade aparente de 100 lotes: '
                      f'{teste_5[0]}\n'
                      f'Envio de oferta de compra de {symbol_bmf} com o mesmo preço da oferta com lote aparente: '
                      f'{teste_6[0]}\n'
                      f'\n\n')


def processmsg_market_bov(symbol_bov, preco_market_bov, preco_banda_sup_bov):

    new_request_bov_stop = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'STOP_LIMIT',
        'Symbol': symbol_bov,
        'Side': 'BUY',
        'Quantity': 2000,
        'TimeInForce': 'DAY',
        'Price': int(preco_market_bov * 1.06),
        "StopTriggerPrice": int(preco_market_bov * 1.06)
    }

    new_request_bov_acima = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'LIMIT',
        'Symbol': 'KLBN11',
        'Side': 'BUY',
        'Quantity': 2000,
        'TimeInForce': 'DAY',
        'Price': (preco_banda_sup_bov + 1),
    }

    new_request_bov_moc = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'Symbol': 'KLBN11',
        'Side': 'BUY',
        'Quantity': 2000,
        'TimeInForce': 'MOC'
    }

    teste_1_bov_stop = envio_stop_market(new_request_bov_stop)

    teste_2_bov_banda_acima = envio_acima_banda_bov(new_request_bov_acima)
    print(f"processmsg_market_bov: {new_request_bov_stop}")

    teste_3_bov_moc = moc_leilao(new_request_bov_moc)

    with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_BOV_Neg.txt", "w") as arquivo:
        arquivo.write(f'****** Fase Negociação BOV ******\n\n'
                      f'Envio de oferta STOP de compra no mercado a vista: '
                      f'{teste_1_bov_stop[0]}\n'
                      f'Envio de compra com preço acima do limite intradiário no mercado a vista: '
                      f'{teste_2_bov_banda_acima[0]}\n'
                      f'Envio de oferta a mercado durante o call de fechamento MOC para um ativo no mercado a vista: '
                      f'{teste_3_bov_moc[0]}\n'
                      )
    print(f'****** Fase Negociação BOV ******\n\n'
          f'Envio de oferta STOP de compra no mercado a vista: '
          f'{teste_1_bov_stop[0]}\n'
          f'Envio de compra com preço acima do limite intradiário no mercado a vista: '
          f'{teste_2_bov_banda_acima[0]}\n'
          f'Envio de oferta a mercado durante o call de fechamento MOC para um ativo no mercado a vista: '
          f'{teste_3_bov_moc[0]}\n'
          )
    envia_basket()


def processmsg_close(symbol_bov, preco_leilao_bov):

    new_request_bov = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'LIMIT',
        'Symbol': symbol_bov,
        'Side': 'BUY',
        'Quantity': 300,
        'TimeInForce': 'DAY',
        'Price': preco_leilao_bov
    }

    new_request_moa_bov = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'Symbol': symbol_bov,
        'Side': 'BUY',
        'Quantity': 2000,
        'TimeInForce': 'MOA',
        'Price': preco_leilao_bov
    }

    new_request_bov_fora_leilao = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'LIMIT',
        'Symbol': symbol_bov,
        'Side': 'BUY',
        'Quantity': 200,
        'TimeInForce': 'DAY',
        'Price': int(preco_leilao_bov * 0.96)
    }

    new_request_bov_stop = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'STOP_LIMIT',
        'Symbol': symbol_bov,
        'Side': 'BUY',
        'Quantity': 200,
        'TimeInForce': 'DAY',
        'Price': int(preco_leilao_bov * 1.04),
        "StopTriggerPrice": int(preco_leilao_bov * 1.04)
    }

    new_request_display = {
        'Broker': BROKER,
        'Account': ACCOUNT,
        'OrderType': 'LIMIT',
        'Symbol': symbol_bov,
        'Side': 'BUY',
        'Quantity': 5000,
        'TimeInForce': 'DAY',
        'Price': preco_leilao_bov,
        'DisplayQuantity': 1000
    }

    update_request_preco_menor_bov = {
        'Price': int(preco_leilao_bov - 12)
    }

    update_request_preco_maior_bov = {
        'Price': int(preco_leilao_bov + 12)
    }

    update_request_preco_menor_bov_fora = {
        'Price': int(preco_leilao_bov * 0.93)
    }

    update_request_diminuindo_bov = {
        'Quantity': 100
    }

    update_request_aumentando_bov = {
        'Quantity': 800
    }

    teste_2 = algo_leilao(update_request_preco_menor_bov,
                          update_request_preco_maior_bov,
                          update_request_diminuindo_bov,
                          update_request_aumentando_bov,
                          new_request_bov)

    teste_3e4_bov = algo_fora_leilao_bmf(update_request_preco_menor_bov_fora,
                                         update_request_diminuindo_bov,
                                         new_request_bov_fora_leilao)

    teste_3_bov = cancela_leilao(new_request_bov)

    teste_4 = moa_leilao(new_request_moa_bov)

    teste_6 = envio_stop(new_request_bov_stop)

    teste_5 = envio_stop(new_request_display)

    print(f'\n\nTESTE BOV {symbol_bov}\n'
          f'Envio de oferta a mercado durante o leilão (MOA): {teste_4[0]}\n'
          f'Modificação de uma oferta dentro do preço teórico, piorando o preço:{teste_2[0]}\n'
          f'Modificação de uma oferta de compra dentro do preço teórico, aumentando quantidade:{teste_2[3]}\n'
          f'Modificação de oferta fora do preço teórico, piorando o preço: {teste_3e4_bov[0]}\n'
          f'Cancelamento de oferta de venda com preço participando do teórico (antes): {teste_3_bov[0]}\n'
          f'Envio de oferta com quantidade aparente: {teste_5[0]} \n'
          f'Envio de oferta STOP: {teste_6[0]}')

    with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_close.txt", "w") as arquivo:
        arquivo.write(f'****** Fechamento ******\n\n'
                      f'Envio de oferta a mercado durante o leilão (MOA): {teste_4[0]}\n'
                      f'Modificação de uma oferta dentro do preço teórico, piorando o preço:{teste_2[0]}\n'
                      f'Modificação de uma oferta de compra dentro do preço teórico, aumentando quantidade:'
                      f'{teste_2[0]}\n'
                      f'Modificação de oferta fora do preço teórico, piorando o preço: {teste_3e4_bov[0]}\n'
                      f'Cancelamento de oferta de venda com preço participando do teórico (antes): {teste_3_bov[0]}\n'
                      f'Envio de oferta com quantidade aparente: {teste_5[0]} \n'
                      f'Envio de oferta STOP: {teste_6[0]}\n')

    envia_basket_leilao()
    time.sleep(80)
    teste_7_bov = cancela_leilao_after(new_request_bov)
    with open("P:/Python ETS/MOCK_pre_abertura/teste_MOCK_close.txt", "a") as arquivo:
        arquivo.write(f'Cancelamento de oferta de venda com preço participando do teórico: {teste_7_bov[0]}\n'
                      f'\n\n')

    # envia_evidencias()


#######################################################################################################################

"""Fase de negociação"""

isFirstMessage = True
StatusOpenBov = True
StatusOpenBmf = True
StatusMarketBov = True
StatusMarketBmf = True
StatusMoa = True
StatusClose = True
StatusFinish = True
count_market = 0

dict_symbols = {}

planilha = pd.read_excel('P:/Python ETS/MOCK_pre_abertura/Ordens.xlsx')
lista = planilha['papeis_completos'].tolist()
k = 0
lista2 = []
while type(lista[k]) == str:
    lista2.append(lista[k])
    k += 1

for symbol in lista2:

    dict_symbol = {symbol: {'LastTradePrice': "",
                            'AuctionPrice': "",
                            'TradingStatusCode': "",
                            'LimitSup': "",
                            'LimitInf': ""}}

    dict_symbols.update(dict_symbol)


def get_token():
    response = requests.post(TOKEN_URL, data=token_request)
    response.raise_for_status()
    return response.json()['access_token']


def set_low_limit_price(RejectionLowLimitPercent, HardLowLimitPrice, LastTradePrice):
    if RejectionLowLimitPercent > 0 and HardLowLimitPrice == 0:
        LowLimitPrice = RejectionLowLimitPercent
    elif RejectionLowLimitPercent == 0 and HardLowLimitPrice == 0:
        LowLimitPrice = 0
    else:
        rejectionLowLimit = LastTradePrice * (RejectionLowLimitPercent / 100 + 1)
        LowLimitPrice = max(rejectionLowLimit, HardLowLimitPrice)
    return LowLimitPrice


def set_high_limit_price(RejectionHighLimitPercent, HardHighLimitPrice, LastTradePrice, RejectionLowLimitPercent):
    if RejectionLowLimitPercent > 0 and HardHighLimitPrice == 0:
        HighLimitPrice = RejectionHighLimitPercent
    elif RejectionLowLimitPercent == 0 and HardHighLimitPrice == 0:
        HighLimitPrice = 0
    else:
        rejectionHighLimit = LastTradePrice * (RejectionHighLimitPercent / 100 + 1)
        HighLimitPrice = min(rejectionHighLimit, HardHighLimitPrice)
    return HighLimitPrice


def on_message(ws, message):
    global StatusFinish
    global StatusMarketBov
    global StatusMarketBmf
    global StatusOpenBov
    global StatusOpenBmf
    global StatusClose
    global isFirstMessage
    global dict_symbols
    global count_market

    if isFirstMessage:
        print(message)
        isFirstMessage = False
        return
    if message == b'\xff':
        ws.send(b'1')
        return
    message_des = msgpack.unpackb(message, timestamp=3)  # to get datetime with timezone info

    dict_symbols[message_des["Symbol"]]["LimitInf"] = set_low_limit_price(message_des['RejectionLowLimitPercent'],
                                                                          message_des['HardLowLimitPrice'],
                                                                          message_des['LastTradePrice'])

    dict_symbols[message_des["Symbol"]]["LimitSup"] = set_high_limit_price(message_des['RejectionHighLimitPercent'],
                                                                           message_des['HardHighLimitPrice'],
                                                                           message_des['LastTradePrice'],
                                                                           message_des['RejectionLowLimitPercent'])

    dict_symbols[message_des["Symbol"]]["LastTradePrice"] = message_des['LastTradePrice']
    dict_symbols[message_des["Symbol"]]["AuctionPrice"] = message_des['AuctionPrice']
    dict_symbols[message_des["Symbol"]]["TradingStatusCode"] = message_des['TradingStatusCode']

    if count_market > 9:
        count_market = 0
    else:
        count_market += 1

    print(count_market)

    if message_des["Symbol"] in plan['papeis_bov'].tolist() and StatusOpenBov is True and message_des[
        'TradingStatusCode'] == 21 and type(dict_symbols[message_des["Symbol"]]["AuctionPrice"]) == float:
        StatusOpenBov = False
        print(dict_symbols)
        p1 = Process(target=processmsg_bov, args=(message_des["Symbol"],
                                                  dict_symbols[message_des["Symbol"]]["AuctionPrice"]))
        p1.start()
    if message_des["Symbol"] in plan['papeis_bmf'].tolist() and StatusOpenBmf is True and message_des[
        'TradingStatusCode'] == 21 and type(dict_symbols[message_des["Symbol"]]["AuctionPrice"]) == float:
        StatusOpenBmf = False
        print(dict_symbols)
        p3 = Process(target=processmsg_bmf, args=(message_des["Symbol"],
                                                  dict_symbols[message_des["Symbol"]]["AuctionPrice"]))
        p3.start()
    if message_des["Symbol"] in plan['papeis_bov'].tolist() and StatusMarketBov is True and message_des[
        'TradingStatusCode'] == 17 and type(dict_symbols[message_des["Symbol"]]["LastTradePrice"]) == float:
        StatusMarketBov = False
        print(dict_symbols)
        p4 = Process(target=processmsg_market_bov, args=(message_des["Symbol"],
                                                         dict_symbols[message_des["Symbol"]]["LastTradePrice"],
                                                         int(dict_symbols[message_des["Symbol"]]["LimitSup"])))
        p4.start()
    if message_des["Symbol"] in plan['papeis_bmf'].tolist() and StatusMarketBmf is True and message_des[
        'TradingStatusCode'] == 17 and type(dict_symbols[message_des["Symbol"]]["LastTradePrice"]) == float:
        StatusMarketBmf = False
        print(dict_symbols)
        p6 = Process(target=processmsg_market_bmf, args=(message_des["Symbol"],
                                                         dict_symbols[message_des["Symbol"]]["LastTradePrice"]))
        p6.start()
    if message_des["Symbol"] in plan['papeis_bov'].tolist() and StatusClose is True and message_des[
        'TradingStatusCode'] == 101 and type(dict_symbols[message_des["Symbol"]]["AuctionPrice"]) == float:
        StatusClose = False
        print(dict_symbols)
        p7 = Process(target=processmsg_close, args=(message_des["Symbol"],
                                                    dict_symbols[message_des["Symbol"]]["AuctionPrice"]))
        p7.start()
    # if StatusOpenBov is not True and StatusOpenBmf is not True and StatusMarketBov is not True and StatusMarketBmf is not True and StatusClose is not True and StatusFinish is True:
        # ws.close()
        # StatusFinish = False


def on_open(ws):
    ws.send(json.dumps({"Token": get_token(), "Symbols": lista2, "UpdateFreq": updateFreq}))


def main():
    websocket.enableTrace(False)
    websocket_base_url = BASE_URL_WS.replace('http', 'ws')
    data = "bestoffers"
    endpoint_address = f'{websocket_base_url}/{data}'
    ws = websocket.WebSocketApp(endpoint_address,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error)
    ws.on_open = on_open
    ws.run_forever()


if __name__ == '__main__':
    freeze_support()
    main()
