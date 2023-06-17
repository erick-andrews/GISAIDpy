"""
Script that contains and defines the primary functions needed for GISAIDpy implementation.

"""

import logging
import warnings
import requests
import urllib.parse
import json
import time
import polars as pl
from constants import GISAID

def timestamp():
    return f"{int(time.time() * 1000)}"

def create_command(wid, pid, cid, cmd, params=None, equiv=None):
    ev = {
        'wid': wid,
        'pid': pid,
        'cid': cid,
        'cmd': cmd,
        'params': params if params is not None else {},
        'equiv': equiv
    }
    return ev

def format_data_for_request(sid, wid, pid, queue, timestamp, mode='ajax'):
    data = f"sid={sid}&wid={wid}&pid={pid}&data={urllib.parse.quote(json.dumps(queue), safe='~()*!.')}&ts={timestamp}&mode={mode}"
    return data

def parse_response(res):
    j = res.json()
    response_data = j['responses'][0]['data']

    error_messages = {
        'Error': "Internal server error.",
        'expired': "The session has expired. Please login again.",
        'password': "Username or password is wrong!",
        'No data.': "No data found."
    }

    for error_type, error_message in error_messages.items():
        if error_type in response_data:
            if error_type == 'Error':
                issue_link = "https://github.com/Wytamma/GISAIDR/issues/1"
                warnings.warn(f"There was an error, previously documented by Wytamma. Please see link for R issue: {issue_link}")
            else:
                warnings.warn(error_message)
            raise Exception(error_message)

    return j

def extract_first_match(regex, text):
    logging.debug(f"Extracting '{regex}' from '{text[:30]}'")
    match = re.search(regex, text)
    if match:
        return match.group(1)
    else:
        return None
    
def send_back_cmd(session_id, WID, PID, CID):
    # send back command to get back to page
    selection_command = create_command(
        wid=WID,
        pid=PID,
        cid=CID,
        cmd='Back',
        params={}
    )
    queue = {'queue': [selection_command]}

    data = format_data_for_request(session_id, WID, PID, queue, timestamp())

    response = send_request(method='POST', data=data)

    response_data = parse_response(response)

def reset_query(credentials):
    queue = []
    command = create_command(
        wid=credentials['wid'],
        pid=credentials['pid'],
        cid=credentials['search_cid'],
        cmd='Reset'
    )
    queue.append(command)
    command_queue = {'queue': queue}

    data = format_data_for_request(
        sid=credentials['sid'],
        wid=credentials['wid'],
        pid=credentials['pid'],
        queue=command_queue,
        timestamp=timestamp()
    )

    res = requests.post(GISAID.GISAID_URL, headers=GISAID.HEADERS, data=data)

    return res

def get_download_panel(session_id, WID, customSearch_page_ID, query_cid):
    selection_command = create_command(
        wid=WID,
        pid=customSearch_page_ID,
        cid=query_cid,
        cmd='DownloadAllSequences',
        params={}
    )
    queue = {'queue': [selection_command]}

    data = format_data_for_request(
        sid=session_id,
        wid=WID,
        pid=customSearch_page_ID,
        queue=queue,
        timestamp=timestamp()
    )

    response = send_request(method='POST', data=data)
    response_data = parse_response(response)
    logging.debug(f"get_download_panel_pid_wid (response_data): {response_data}")

    download_pid = response_data['responses'][0]['data'].split("'")[3]
    download_wid = response_data['responses'][0]['data'].split("'")[1]

    return {'pid': download_pid, 'wid': download_wid}

def get_accession_ids(credentials):
    command_queue = {
        'queue': [
            create_command(
                wid=credentials['wid'],
                pid=credentials['pid'],
                cid=credentials['query_cid'],
                cmd='CallAsync',
                params={'col_name': 'c', 'checked': True, '_async_cmd': 'SelectAll'}
            )
        ]
    }

    data = format_data_for_request(
        sid=credentials['sid'],
        wid=credentials['wid'],
        pid=credentials['pid'],
        queue=command_queue,
        timestamp=timestamp()
    )
    res = requests.post(GISAID.GISAID_URL, headers=GISAID.HEADERS, data=data)
    j = parse_response(res)

    check_async_id = j['callback_response']['async_id']
    while True:
        res = requests.get(f"https://www.epicov.org/epi3/check_async/{check_async_id}?_={timestamp()}")
        j = parse_response(res)
        if j['__ready__']:
            break
        time.sleep(1)

    logging.debug(j)

    selection_pid_wid = get_selection_panel(credentials['sid'], credentials['wid'], credentials['pid'], credentials['query_cid'])
    selection_page = send_request(f"sid={credentials['sid']}&pid={selection_pid_wid['pid']}")

    command_queue = {
        'queue': [
            create_command(
                wid=selection_pid_wid['wid'],
                pid=selection_pid_wid['pid'],
                cid=credentials['selection_panel_cid'],
                cmd='Download',
                params={}
            )
        ]
    }

    data = format_data_for_request(
        sid=credentials['sid'],
        wid=credentials['wid'],
        pid=credentials['pid'],
        queue=command_queue,
        timestamp=timestamp()
    )
    res = requests.post(GISAID.GISAID_URL, headers=GISAID.HEADERS, data=data)
    j = parse_response(res)
    url = extract_first_match(r"sys.downloadFile\(\"(.*)\",", j['responses'][0]['data'])
    logging.debug(f"https://www.epicov.org/{url}")
    response = requests.get(f"https://www.epicov.org/{url}")
    df = pl.read_csv(response.content.decode(), header=None)
    df = df.rename(['accession_id'])

    send_back_cmd(credentials['sid'], selection_pid_wid['wid'], selection_pid_wid['pid'], credentials['selection_panel_cid'])
    reset_query(credentials)
    return df

def get_selection_panel(session_id, WID, customSearch_page_ID, query_cid):
    selection_command = create_command(
        wid=WID,
        pid=customSearch_page_ID,
        cid=query_cid,
        cmd='Selection',
        params={}
    )
    queue = {'queue': [selection_command]}

    data = format_data_for_request(session_id, WID, customSearch_page_ID, queue, timestamp())

    response = send_request(method='POST', data=data)

    response_data = parse_response(response)
    selection_pid = response_data['responses'][0]['data'].split("'")[3]
    selection_wid = response_data['responses'][0]['data'].split("'")[1]

    logging.debug(f"get_selection_panel (response_data): {response_data}")

    return {'pid': selection_pid, 'wid': selection_wid}

def send_request(parameter_string="", data=None, method='GET'):
    URL = GISAID.GISAID_URL + '?' + parameter_string
    if data is None:
        data = ""
    logging.debug(f"Sending request:\n Method -> {method}\n URL -> {URL}\n data -> {data}")
    
    if method == 'GET':
        response = requests.get(URL)
    elif method == 'POST':
        response = requests.post(URL, headers=GISAID.HEADERS, data=data)
    else:
        raise ValueError(f"Method '{method}' not allowed")
    
    if response.status_code >= 500:
        logging.warning(f"An error occurred while trying to {method} {URL}")
        raise Exception("Server error!")
    
    return response

def select_entries(credentials, list_of_accession_ids):
    accession_ids_string = ", ".join(list_of_accession_ids)

    selection_pid_wid = get_selection_panel(credentials['sid'], credentials['wid'], credentials['pid'], credentials['query_cid'])

    # Load panel
    selection_page = send_request(f"sid={credentials['sid']}&pid={selection_pid_wid['pid']}")

    ev1 = create_command(
        wid=selection_pid_wid['wid'],
        pid=selection_pid_wid['pid'],
        cid=credentials['selection_panel_cid'],
        cmd='setTarget',
        params={'cvalue': accession_ids_string, 'ceid': credentials['selection_ceid']},
        equiv=f"ST{credentials['selection_ceid']}"
    )

    ev2 = create_command(
        wid=selection_pid_wid['wid'],
        pid=selection_pid_wid['pid'],
        cid=credentials['selection_panel_cid'],
        cmd='ChangeValue',
        params={'cvalue': accession_ids_string, 'ceid': credentials['selection_ceid']},
        equiv=f"CV{credentials['selection_ceid']}"
    )

    ev3 = create_command(
        wid=selection_pid_wid['wid'],
        pid=selection_pid_wid['pid'],
        cid=credentials['selection_panel_cid'],
        cmd='OK',
        params={}
    )

    json_queue = {'queue': [ev1, ev2, ev3]}
    data = format_data_for_request(
        credentials['sid'],
        selection_pid_wid['wid'],
        selection_pid_wid['pid'],
        json_queue,
        timestamp()
    )
    response = send_request(method='POST', data=data)
    response_data = parse_response(response)
    logging.debug(response_data)
    
    if 'Back' in response_data['responses'][1]['data']:
        send_back_cmd(
            credentials['sid'],
            selection_pid_wid['wid'],
            selection_pid_wid['pid'],
            credentials['selection_panel_cid']
        )
    
    return response