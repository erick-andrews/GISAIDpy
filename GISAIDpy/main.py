"""
Primary script in GISAIDpy, combines functions to produce API request and downloads.

"""
from functions import *
import glob
import re
import requests
import click
import numpy as np
import polars as pl
from polars_funcs import read_fasta
import urllib.request
import tarfile
import os
import shutil

def download(credentials, list_of_accession_ids, get_sequence=TRUE, clean_up=TRUE):
    """
    Function for downloading data from whichever GISAID db you would like to access.
    
    Args:
        credentials ()
    """
    if len(list_of_accession_ids) > 5000:
        raise ValueError('Can only download a maximum of 5000 samples at a time.')
    elif len(list_of_accession_ids) == 0:
        raise ValueError('Select at least one sequence!')
    
    print('Selecting entries...')

    response = select_entries(credentials=credentials, list_of_accession_ids=list_of_accession_ids)

    download_pid_wid = {'pid': credentials['pid'], 'wid': credentials['wid']}
    download_cmd = 'Download'
    download_pid_wid = get_download_panel(credentials['sid'], credentials['wid'], credentials['pid'], credentials['query_cid'])
    # load panel
    download_page = send_request(f"sid={credentials['sid']}&pid={download_pid_wid['pid']}")

    download_page_text = download_page.text

    if credentials['database'] == 'EpiRSV':
        credentials['download_panel_cid'] = re.search(r"'(.{5,20})','RSVDownloadSelectionComponent", download_page_text).group(1)
    elif credentials['database'] == 'EpiPox':
        credentials['download_panel_cid'] = re.search(r"'(.{5,20})','MPoxDownloadSelectionComponent", download_page_text).group(1)
    else:
        credentials['download_panel_cid'] = re.search(r"'(.{5,20})','DownloadSelectionComponent", download_page_text).group(1)
        radio_button_widget_cid = re.search(r"'(.{5,20})','RadiobuttonWidget", download_page_text).group(1)

    queue = []
    command = create_command(
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        cid=credentials['download_panel_cid'],
        cmd='setTarget',
        params={'cvalue': 'augur_input', 'ceid': radio_button_widget_cid}
    )
    queue.append(command)

    command = create_command(
    wid=download_pid_wid['wid'],
    pid=download_pid_wid['pid'],
    cid=credentials['download_panel_cid'],
    cmd='ChangeValue',
    params={'cvalue': 'augur_input', 'ceid': radio_button_widget_cid}
    )
    queue.append(command)

    command = create_command(
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        cid=credentials['download_panel_cid'],
        cmd='FormatChange',
        params={'ceid': radio_button_widget_cid}
    )
    queue.append(command)

    command_queue = {'queue': queue}
    data = format_data_for_request(
        credentials['sid'],
        download_pid_wid['wid'],
        download_pid_wid['pid'],
        command_queue,
        timestamp()
    )
    response = send_request(method='POST', data=data)
    # make json object of parsed response
    response_data = parse_response(response)

    ev = create_command(
    wid=download_pid_wid['wid'],
    pid=download_pid_wid['pid'],
    cid=credentials['download_panel_cid'],
    cmd='DownloadReminder',
    params={}
    )
    json_queue = {'queue': [ev]}
    data = format_data_for_request(
        credentials['sid'],
        download_pid_wid['wid'],
        download_pid_wid['pid'],
        json_queue,
        timestamp()
    )
    res = send_request(method='POST', data=data)
    j = parse_response(res)

    download_pid_wid['wid'] = extract_first_match(r"sys.openOverlay\('(.{5,20})',", j['responses'][2]['data'])
    download_pid_wid['pid'] = extract_first_match(r",'(.{5,20})',new Object", j['responses'][2]['data'])

    agreement_page = send_request(f"sid={credentials['sid']}&pid={download_pid_wid['pid']}&wid={download_pid_wid['wid']}&mode=page", method="POST")
    agreement_page_text = agreement_page.text
    credentials['download_panel_cid'] = extract_first_match(r"'(.{5,20})','Corona2020DownloadReminderButtonsComponent", agreement_page_text)
    agree_check_box_ceid = extract_first_match(r"createFI\('(.{5,20})','CheckboxWidget'", agreement_page_text)
    

    # new queue for new command concatenation
    queue = []

    command = create_command(
    wid=download_pid_wid['wid'],
    pid=download_pid_wid['pid'],
    cid=credentials['download_panel_cid'],
    cmd='setTarget',
    params={'cvalue': ['agreed'], 'ceid': agree_check_box_ceid}
    )
    queue.append(command)

    command = create_command(
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        cid=credentials['download_panel_cid'],
        cmd='ChangeValue',
        params={'cvalue': ['agreed'], 'ceid': agree_check_box_ceid}
    )
    queue.append(command)

    command = create_command(
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        cid=credentials['download_panel_cid'],
        cmd='Agreed',
        params={'ceid': agree_check_box_ceid}
    )
    queue.append(command)

    command_queue = {'queue': queue}
    data = format_data_for_request(
        sid=credentials['sid'],
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        queue=command_queue,
        timestamp=timestamp()
    )
    response = send_request(method='POST', data=data)
    response_data = parse_response(response)

    logging.debug(response_data)

    ev = create_command(
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        cid=credentials['download_panel_cid'],
        cmd=download_cmd,
        params={}  # hack for empty {}, direct holdover from R
    )
    json_queue = {'queue': [ev]}
    data = format_data_for_request(
        sid=credentials['sid'],
        wid=download_pid_wid['wid'],
        pid=download_pid_wid['pid'],
        queue=json_queue,
        timestamp=timestamp()
    )
    print('Compressing data. Please wait...')
    res = send_request(method='POST', data=data)
    j = parse_response(res)

    # Send POST request
    res = requests.post(GISAID.GISAID_URL, headers=GISAID.HEADERS, data=data)
    j = parse_response(res)

    # Extract check_async
    check_async_id = j['responses'][0]['data'].split("'")[3]

    # Wait until generateDownloadDone is ready
    is_ready = False
    while not is_ready:
        res = requests.get(f"https://www.epicov.org/epi3/check_async/{check_async_id}?_={timestamp()}")
        j = parse_response(res)
        is_ready = j['is_ready']
        if not is_ready:
            time.sleep(1)

    # Get download link
    print('Data ready.')
    ev = create_command(
        wid=credentials['wid'],
        pid=credentials['pid'],
        cid=credentials['query_cid'],
        cmd="generateDownloadDone",
        params={}
    )
    json_queue = {'queue': [ev]}
    data = format_data_for_request(credentials['sid'], credentials['wid'], credentials['pid'], json_queue, timestamp())
    res = send_request(method='POST', data=data)
    # get final response data for download url
    j = parse_response(res)

    # Extract download URL
    download_url = "https://www.epicov.org" + j['responses'][0]['data'].split('"')[1]

    # Attempt download
    print('Downloading...')
    if credentials['database'] == 'EpiCoV':
        tmpTarFile = 'gisaidr_data_tmp.tar'
        urllib.request.urlretrieve(download_url, tmpTarFile)
        with tarfile.open(tmpTarFile, 'r') as tar:
            tar.extractall('gisaidr_data_tmp')
        metadataFile = glob.glob('gisaidr_data_tmp/*.metadata.tsv')[0]
        if metadataFile is None:
            print('gisaid_data files:')
            print(glob.glob('gisaidr_data_tmp/*'))
            raise Exception('Could not find metadata file.')
        df = pl.read_csv(metadataFile, sep='\t', quotechar='')
        df = df.sort('gisaid_epi_isl', reverse=True)
        df = df.rename({'gisaid_epi_isl': 'accession_id'})
        if get_sequence:
            sequencesFile = glob.glob('gisaidr_data_tmp/*.sequences.fasta')[0]
            if sequencesFile is None:
                raise Exception('Could not find sequences file.')
            seq_df = read_fasta(sequencesFile)
            df = df.join(seq_df, on='strain', how='outer')
    else:
        sequencesFile = 'gisaidr_data_tmp.fasta'
        urllib.request.urlretrieve(download_url, sequencesFile)
        fdf = read_fasta(sequencesFile, get_sequence)
        df = pl.concat([pl.DataFrame({'strain': strain.split('|')}) for strain in fdf['strain']])
        df = df.reset_index(drop=True)
        df = df.with_columns({
            'strain': pl.col('strain'),
            'accession_id': pl.col('accession_id'),
            'collection_date': pl.col('collection_date'),
            'description': pl.col('description')
        })
        if get_sequence:
            df = df.with_column('sequence', pl.col('sequence'))

    # Clean up
    if clean_up:
        if credentials['database'] == 'EpiCoV':
            if os.path.exists(tmpTarFile):
                os.remove(tmpTarFile)
            if os.path.exists('gisaidr_data_tmp'):
                shutil.rmtree('gisaidr_data_tmp')
        else:
            if os.path.exists(sequencesFile):
                os.remove(sequencesFile)

    df = df.replace('?', pl.NA)

    return df


if __name__ == "__main__":
    download(credentials, list_of_accession_ids)