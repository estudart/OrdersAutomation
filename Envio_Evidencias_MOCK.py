from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, From, Attachment, FileContent, FileName, FileType, Disposition)
import pandas as pd
import os
import base64
from unidecode import unidecode
from selenium import webdriver
from selenium.webdriver.common.by import By
import glob
import time


def envia_evidencias():
    wait = 25

    SENDGRID_KEY = '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'
    sg = SendGridAPIClient(SENDGRID_KEY)
    sender = 'ets@americastg.com'

    # Planilha que contém os reponsáveis por receber as evidências do MOCK
    evidencia_email = pd.read_excel("P:/Python ETS/MOCK_pre_abertura/Script_Stag/email_evidencia_mock.xlsx")
    call = str(evidencia_email.loc[0, 'chamado'])
    user = str(evidencia_email.loc[0, 'user'])
    # URL_Staging = "http://atgadmin-staging.americastg.com/Login"
    URL_Prod = "https://atgadmin.americastg.com.br/Login"

    # Abre o navegador do Chrome
    navegador = webdriver.Chrome()

    # Entra no site do Admin da ATG
    navegador.get(URL_Prod)
    # login_staging = ['estudart', "ericada@123"]
    login_prod = {"username": "atg_erico", "password": "ERICO@123@studart"}

    time.sleep(5)

    # Preenche login e senha
    navegador.find_element(By.XPATH, '//*[@id="UserName"]').send_keys(login_prod["username"])
    navegador.find_element(By.XPATH, '//*[@id="Password"]').send_keys(login_prod["password"])

    # Clica em logar
    navegador.find_element(By.XPATH, '//*[@id="Login"]').click()
    time.sleep(3)

    # Clica em MTB "All Orders"
    navegador.find_element(By.XPATH, '//*[@id="NavigationMenu"]/ul/li[2]/a').click()
    navegador.find_element(By.XPATH, '//*[@id="NavigationMenu"]/ul/li[2]/ul/li[5]/a').click()
    time.sleep(25)

    # Seleciona a corretora CM Capital
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select').click()
    # Clica em pesquisar, para CM Capital
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select/option[22]').click()
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/button[1]').click()
    time.sleep(5)
    # Clica para exportar todas as paginas
    navegador.find_element(By.XPATH, '//*[@id="ToolTables_DataTables_Table_0_1"]').click()
    time.sleep(wait)

    # Seleciona a corretora INTL FCSTONE
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select').click()
    # Clica em pesquisar, para INTL FCSTONE
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select/option[59]').click()
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/button[1]').click()
    time.sleep(4)
    # Clica para exportar todas as paginas
    navegador.find_element(By.XPATH, '//*[@id="ToolTables_DataTables_Table_0_1"]').click()
    time.sleep(wait)

    # Seleciona a corretora SANTANDER
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select').click()
    # Clica em pesquisar, para SANTANDER
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select/option[92]').click()
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/button[1]').click()
    time.sleep(4)
    # Clica para exportar todas as paginas
    navegador.find_element(By.XPATH, '//*[@id="ToolTables_DataTables_Table_0_1"]').click()
    time.sleep(wait)

    # Seleciona a corretora MERRILL LYNCH
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select').click()
    # Clica em pesquisar, para MERRILL LYNCH
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select/option[72]').click()
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/button[1]').click()
    time.sleep(4)
    # Clica para exportar todas as paginas
    navegador.find_element(By.XPATH, '//*[@id="ToolTables_DataTables_Table_0_1"]').click()
    time.sleep(wait)


    # Seleciona todas as corretoras
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select').click()
    # Clica em pesquisar, para All Brokers
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/select/option[3]').click()
    time.sleep(15)
    navegador.find_element(By.XPATH, '//*[@id="ng-app"]/div[1]/div[2]/div/div[1]/button[1]').click()
    time.sleep(20)
    # Clica para exportar todas as paginas
    # Para fins de teste utilizar apenas uma pagina
    # navegador.find_element(By.XPATH, '//*[@id="ToolTables_DataTables_Table_0_0"]').click()
    navegador.find_element(By.XPATH, '//*[@id="ToolTables_DataTables_Table_0_1"]').click()
    time.sleep(wait)

    now = datetime.now()
    date = now.strftime("%d/%m/%Y")
    year = date[-2:]
    month = date[3:5]
    day = date[:2]

    data = f"{day}/{month}/{year}"

    list = []

    # Caminho para a pasta Downloads
    caminho_downloads = f"C:/Users/{user}/Downloads/"

    def create_attachment(att_path, att_file, filetype):
        filename = att_file
        att_file = f"{att_path}/{att_file}"

        # checa o caminho
        if not os.path.exists(att_file):
            print(f"Caminho não existe: {att_file}")
            return
        # le o arquivo
        with open(att_file, 'rb') as f:
            data = f.read()
            f.close()

        # codifica o conteudo do arquivo
        encoded_file = base64.b64encode(data).decode()

        # cria o anexo
        return Attachment(
            FileContent(encoded_file),
            FileName(unidecode(filename)),
            FileType(filetype),
            Disposition('attachment')
        )

    # Função que cria o email
    def create_email(broker, receive_email, call):

        is_morning = now.hour < 12
        is_night = now.hour > 18

        if is_morning:
            greeting = "bom dia"
        elif is_night:
            greeting = "boa noite"
        else:
            greeting = "boa tarde"

        email_body = f"""
        
    <span style='font-family:verdana;font-size:12.0;color:#1F497D'>
    
    Prezados, {greeting}.<br><br>
    
    Segue anexo das evidências do MOCK TEST. <br><br>
    
    Qualquer dúvida, favor entrar em contato com a equipe. <br><br>
    
    
    Atenciosamente, <br><br>
    
                """

        subject = f"{broker} | Evidências MOCK TEST {data} - #{call}"

        message = Mail(
            from_email=From(sender, 'ATG | Electronic Trading Support'),
            to_emails=receive_email,
            subject=subject,
            html_content=email_body
        )

        #message.add_cc('servicedesk@americastg.com')
        message.add_cc('ets@americastg.com')

        return message

    list_of_files = filter(os.path.isfile, glob.glob(caminho_downloads + '*'))

    # Sort list of files based on last modification time in ascending order
    list_of_files = sorted(list_of_files, key=os.path.getmtime)

    for file_path in list_of_files:
            timestamp_str = time.strftime('%m/%d/%Y :: %H:%M:%S', time.gmtime(os.path.getmtime(file_path)))
            if "Americas Trading Group" in file_path:
                list.append(file_path)

    cont = 0

    for k in list[-5:]:
        broker = evidencia_email.loc[cont, 'broker']
        receive_email = evidencia_email.loc[cont, 'email_list'].split(";")

        file = k.replace('\\', "/")
        file3 = file.split("/")
        file3 = file3[:-1]
        file3 = "/".join(file3)
        file2 = file.split("/")[-1]

        attachment_evidencia = create_attachment(
            att_path=file3,
            att_file=file2,
            filetype='application/pdf'
        )
        message = create_email(broker, receive_email, call)
        message.add_attachment(attachment_evidencia)
        sg.send(message)

        cont += 1

        time.sleep(3)


envia_evidencias()
