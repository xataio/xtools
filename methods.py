from time import sleep
from datetime import datetime, timezone
import requests
from urllib.parse import urljoin

def get(apikey, urlPath, headers={}, expect_codes=[],ERROR_FILE='',host_header=''):
    headers={}
    headers["Authorization"] = f"Bearer {apikey}"
    if host_header != '':
        headers["Host"] = host_header
    run=True
    errors={}
    while run==True:
        try:
            resp = requests.request("GET", urlPath, headers=headers)
            run = False
            if resp.status_code > 299:
                #Track error
                if resp.status_code in errors:
                    errors[resp.status_code]+=1
                else:
                    errors[resp.status_code]=1
                #Retry upon throttling error
                if resp.status_code == 429:
                    sleep(0.05)
                    run=True
                else:
                    if resp.status_code not in expect_codes:
                        with open(ERROR_FILE, "a") as f:
                            f.writelines([str(datetime.now().isoformat())+" GET request failed ",str(urlPath),"\n",str(resp.status_code)," ",str(resp.text),"\n"])
        except requests.exceptions.ConnectionError as e:
            run=True
    return resp,errors

def post(apikey, urlPath, headers={}, payload='', expect_codes=[],ERROR_FILE='',host_header=''):
    headers={}
    headers["Authorization"] = f"Bearer {apikey}"
    headers["Content-Type"] = f"application/json"
    if host_header != '':
        headers["Host"] = host_header
    run=True
    errors={}
    while run==True:
        try:
            resp = requests.request("POST", urlPath, headers=headers, json=payload)
            run = False
            if resp.status_code > 299:
                #Track error
                if resp.status_code in errors:
                    errors[resp.status_code]+=1
                else:
                    errors[resp.status_code]=1
                #Retry upon throttling error
                if resp.status_code == 429:
                    sleep(0.05)
                    run=True
                else:
                    if resp.status_code not in expect_codes:
                        with open(ERROR_FILE, "a") as f:
                            f.writelines([str(datetime.now().isoformat())+" POST request failed ",str(urlPath),"\n",str(payload),"\n",str(resp.status_code)," ",str(resp.text),"\n"])
        except requests.exceptions.ConnectionError as e:
            run=True
    return resp,errors

def put(apikey, urlPath, headers={}, payload='', expect_codes=[],ERROR_FILE='',host_header=''):
    headers={}
    headers["Authorization"] = f"Bearer {apikey}"
    headers["Content-Type"] = f"application/json"
    if host_header != '':
        headers["Host"] = host_header
    run=True
    errors={}
    while run==True:
        try:
            resp = requests.request("PUT", urlPath, headers=headers, json=payload)
            run = False
            if resp.status_code > 299:
                #Track error
                if resp.status_code in errors:
                    errors[resp.status_code]+=1
                else:
                    errors[resp.status_code]=1
                #Retry upon throttling error
                if resp.status_code == 429:
                    sleep(0.05)
                    run=True
                else:
                    if resp.status_code not in expect_codes:
                        with open(ERROR_FILE, "a") as f:
                            f.writelines([str(datetime.now().isoformat())+" PUT request failed ",str(urlPath),"\n",str(payload),"\n",str(resp.status_code)," ",str(resp.text),"\n"])
        except requests.exceptions.ConnectionError as e:
            run=True
    return resp,errors

def patch(apikey, urlPath, headers={}, payload='', expect_codes=[],ERROR_FILE='',host_header=''):
    headers={}
    headers["Authorization"] = f"Bearer {apikey}"
    headers["Content-Type"] = f"application/json"
    if host_header != '':
        headers["Host"] = host_header
    run=True
    errors={}
    while run==True:
        try:
            resp = requests.request("PATCH", urlPath, headers=headers, json=payload)
            run = False
            if resp.status_code > 299:
                #Track error
                if resp.status_code in errors:
                    errors[resp.status_code]+=1
                else:
                    errors[resp.status_code]=1
                #Retry upon throttling error
                if resp.status_code == 429:
                    sleep(0.05)
                    run=True
                else:
                    if resp.status_code not in expect_codes:
                        with open(ERROR_FILE, "a") as f:
                            f.writelines([str(datetime.now().isoformat())+" PATCH request failed ",str(urlPath),"\n",str(payload),"\n",str(resp.status_code)," ",str(resp.text),"\n"])
        except requests.exceptions.ConnectionError as e:
            run=True
    return resp,errors