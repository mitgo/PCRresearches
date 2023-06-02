import json
import cred
import requests
import pyodbc
from datetime import datetime, timedelta
import plotly.graph_objects as go
import pandas as pd

icmid = cred.icmid
icmid2 = cred.icmid2

conn = cred.conn

cursor = conn.cursor()
cursor.execute("select * from dbo.LisPcrRes order by finishdate asc")
row = cursor.fetchall()

url = cred.url
url2 = cred.url2

headers = {
    'Content-type': 'application/json',
    'User-Agent': 'akka-http/10.1.6',
    'Content-Type': 'application/json',
    'icmid': 'efe8d6fd-a91e-4ef2-a198-43879dd4894a',
    'apikey': '5f4292c4-686f-47ff-a233-57a05e2fbd75'
}

for i in range(len(row)):
    mystr = {
        "collectInfo": {
            "collectorID": "",
            "collector": "",
            "collectDate": row[i].orderDate
        },
        "diagnosis": "",
        "giveResult": {},
        "icmid": icmid,
        "ids": int(row[i].measurement_id),
        "narrative": "",
        "observationDates": {
            "finish": row[i].finishDate
        },
        "order": {
            "id": "",
            "date": row[i].orderDate,
            "hisId": ""
        },
        "orderInfo": [
            {
                "category": {
                    "code": "",
                    "id": "",
                    "name": ""
                },
                "completed": True,
                "method": "",
                "service": {
                    "code": "1167774",
                    "id": "860",
                    "name": "ПЦР-тестирование на COVID-19 (Экспресс-тест)"
                },
                "workplaceID": ""
            }
        ],
        "OrderingInstitution": {
            "cellPhone": "",
            "code": "742",
            "department": "Стационар",
            "departmentCode": "743",
            "email": "",
            "externalIdentification": [],
            "fullName": "БОльница>",
            "icmid": icmid2,
            "id": "2260459",
            "localName": "ГАУЗ АО Больница",
            "phone": "",
            "physician": {
                "adress": "",
                "cellPhone": "",
                "email": "",
                "familyName": "Врач",
                "givenName": "",
                "id": "2260433",
                "phone": "",
                "regCode": ""
            }
        },
        "originalOrderIdentification": {
            "id": row[i].orderId*10+1,
            "materialId": row[i].orderId*10+2,
            "extId": "",
            "orderid": str(row[i].orderId*10+1)+"01"
        },
        "patient": {
            "address": "",
            "birthDate": row[i].birthDate,
            "conditions": [],
            "email": "",
            "externalID": "",
            "externalIdentification": [],
            "familyName": row[i].familyName,
            "gender": row[i].gender,
            "givenName": row[i].givenName,
            "id": str(row[i].HUM),
            "identifications": [
                {
                    "documentType": "SNILS",
                    "number": row[i].SNILS
                }
            ],
            "insurance": None,
            "mark": "",
            "markID": "",
            "middleName": row[i].middleName,
            "notes": "",
            "phoneNumber": None,
            "province": {
                "code": "",
                "id": "",
                "name": ""
            },
            "regCode": "",
            "regDate": None,
            "snils": row[i].SNILS,
            "telecom": [
                {
                    "cellular": row[i].phoneNumber,
                    "contactType": "CELLULAR"
                }
            ],
            "workPlace": ""
        },
        "purposes": {
            "id": "",
            "name": ""
        },
        "regDate": row[i].orderDate,
        "reports": [
            {
                "finishDate": row[i].finishDate,
                "results": [
                    {
                        "bacteria": [],
                        "codesetID": "",
                        "description": "",
                        "finishDate": row[i].finishDate,
                        "headerMark": "",
                        "mbioType": None,
                        "measurement": {
                            "code": "1167774",
                            "id": "2954",
                            "name": "ПЦР-тестирование на COVID-19 (Экспресс-тест)",
                            "shortName": "ПЦР-тестирование на COVID-19 (Экспресс-тест)"
                        },
                        "norm": {
                            "text": ""
                        },
                        "notes": "",
                        "operator": {
                            "code": "102",
                            "id": 15004,
                            "name": "Врач"
                        },
                        "pathology": "0",
                        "protocol": [],
                        "repLevel": 0,
                        "reportFormat": "",
                        "requested": None,
                        "resCode": "3081",
                        "resCount": 1,
                        "resTable": {
                            "alias": "",
                            "description": "ПЦР-исследования",
                            "id": "",
                            "name": ""
                        },
                        "resultText": row[i].resultText,
                        "resultValue": row[i].resultText,
                        "seqNo": 1,
                        "srvdepCode": "1167774",
                        "unit": "",
                        "unitCode": None,
                        "verifier": {
                            "code": "102",
                            "id ": 15004,
                            "name": "Врач"
                        }
                    }
                ],
                "title": ""
            }
        ],
        "specimenSites": {
            "id": "",
            "name": ""
        },
        "specimenTypes": {
            "id": "2260458",
            "code": "141",
            "name": "Мазок слизистой носоглотки и ротоглотки"
        },
        "status": "F"
    }
    r = requests.post(url, json = mystr, headers=headers)
    # Если выгрузилось успешно - добавляем запись в таблицу хистори об успешной выгрузке
    if (r.status_code == 200) :
        r = requests.post(url2, json=mystr, headers=headers)
        if (r.status_code == 200) :
            s = json.loads(r.text)
            if i % 100 == 0: print(s)
            cursor.execute("insert into PCR_Exported (OrderID, DateOrder, Response) values(" + str(row[i].orderId) + ", convert(datetime, '" + row[i].orderDate + "', 126),'" + s['response'] + "')")
            cursor.commit()
    else:
        print(r.text)
    print(mystr)

def telegram_bot_sendtext(chat_id, bot_message, method):
   message = 'https://api.telegram.org/bot' + cred.bot_token + '/' + method + '?chat_id=' + str(chat_id) + '&parse_mode=Markdown'
   if (method == 'sendPhoto'):
    message += '&caption=' + str(bot_message)
    response = requests.post(message, files = {'photo': open('gr.png', 'rb')})
   else:
    message += '&text='+str(bot_message)
    response = requests.get(message)
   return response.json()

now = datetime.now()
date_format = '%d.%m.%Y'
d_f = '%d.%m'

if (now.hour == 8):
    cursor.execute('SELECT count(*) as count FROM [EHistory].[dbo].[PCR_Exported] where CONVERT(date, DateExport) = CONVERT(date, getdate()-1)')
    count = cursor.fetchall()
    strforsend = "За вчера (__" + (now - timedelta(days = 1)).strftime(date_format)  + "__) в систему РПН выгружено *" + str(count[0].count) + "* результатов исследований Covid-19"
    telegram_bot_sendtext(578300665, strforsend, 'sendMessage')
    telegram_bot_sendtext(5046420533, strforsend, 'sendMessage')
    telegram_bot_sendtext(1729715380, strforsend, 'sendMessage')

if (now.weekday() == 0 and now.hour == 8):
    cursor.execute('SELECT count(*) as count FROM [EHistory].[dbo].[PCR_Exported] where convert(date, DateExport) >= convert(date, getDate() - 7) and convert(date, dateexport) <= convert(date, getDate() - 1)')
    count = cursor.fetchall()
    strforsend = "👮‼За прошлую неделю (__" + (now - timedelta(days = 7)).strftime(d_f) + " - " + (now - timedelta(days = 1)).strftime(d_f) + "__) в РПН выгружено *" + str(count[0].count) + "* результатов исследований Covid-19"
    sql = 'set language N\'russian\' SELECT datename(dw, dateexport) as dw, count(*) as cnt FROM [EHistory].[dbo].[PCR_Exported] where convert(date, DateExport) >= convert(date, getDate() - 7) and convert(date, dateexport) <= convert(date, getDate() - 1) group by datename(dw, dateexport), datepart(dw, dateexport) order by datepart(dw, dateexport)'
    df = pd.read_sql(sql, conn)
    fig = go.Figure (
	go.Bar(x = df['dw'], y = df['cnt'], marker = dict(color = df['cnt']))
    )
    fig.update_layout(legend_title_text = 'Выгрузка результатов ПЦР за неделю')
    fig.update_xaxes(title_text="День недели")
    fig.update_yaxes(title_text="Количество")
    fig.write_image('gr.png')
    telegram_bot_sendtext(cred.user1, strforsend, 'sendPhoto')
    telegram_bot_sendtext(cred.user2, strforsend, 'sendPhoto')
    telegram_bot_sendtext(cred.user3, strforsend, 'sendPhoto')

if (now.weekday() == 4  and now.hour == 12):
    sql = 'set language N\'russian\' SELECT datename(dw, dateexport) as date, count(*) as cnt FROM [EHistory].[dbo].[PCR_Exported] where convert(date, DateExport) >= convert(date, getDate() - 4) and convert(date, dateexport) <= convert(date, getDate()) group by datename(dw, dateexport), datepart(dw, dateexport) order by datepart(dw, dateexport)'
    df = pd.read_sql(sql, conn)
    fig = go.Figure (
        go.Bar(x = df['date'], y = df['cnt'], marker = dict(color = df['cnt']))
    )
    fig.update_layout(legend_title_text = 'Предварительная выгрузка результатов ПЦР за неделю')
    fig.update_xaxes(title_text="День недели")
    fig.update_yaxes(title_text="Количество")
    fig.write_image('gr.png')
    telegram_bot_sendtext(cred.user1, 'Предварительные результаты выгрузок результатов ПЦР за неделю', 'sendPhoto')
    telegram_bot_sendtext(cred.user2, 'Предварительные результаты выгрузок результатов ПЦР за неделю', 'sendPhoto')
    telegram_bot_sendtext(cred.user3, 'Предварительные результаты выгрузок результатов ПЦР за неделю', 'sendPhoto')
