#! python3
import re
import os
import json
import datetime
import argparse


##################################################################################
############# Получение параметров ###############################################
##################################################################################


def get_settings_from_file():
    settings_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "1C_event_log_parser_settings.json")
    
    if not os.path.exists(settings_path):
        settings = {
            'log_path': '.',
            'level': 1,
            'last_parsed_date': '',
            'last_parsed_message': ''
        }
        with open(settings_path, 'w') as outfile:
            json.dump(settings, outfile)
    else:
        with open(settings_path) as json_file:
            settings = json.load(json_file)
            
    return settings
    

##################################################################################
############# Подготовка словаря #################################################
##################################################################################


def add_four_params(params):

    param1 = params.group(1)
    param2 = params.group(2)
    param3 = params.group(3)
    param4 = params.group(4)

    param1_name = get_param_name(param1)
    
    if not param1_name in log_dict:
        log_dict[param1_name] = {'0':{'ID':'','name':''}}

    log_dict[param1_name][param4] = {'ID':param2,'name':param3}
    

def add_three_params(params):
    
    param1 = params.group(1)
    param2 = params.group(2)
    param3 = params.group(3)
    
    param1_name = get_param_name(param1)
    
    if not param1_name in log_dict:
        log_dict[param1_name] = {'0':''}

    log_dict[param1_name][param3] = param2
    

def add_params_from_file():

    dict_file = open(os.path.join(log_path, "1Cv8.lgf"), encoding="utf8")
    end_of_event_regex = re.compile(r'}(,\n|$)')
    
    full_line = ''
    while True:

        cur_line = dict_file.readline()

        if not cur_line:
            break
        
        if not cur_line.startswith('{'):
            continue

        full_line = full_line + cur_line

        if end_of_event_regex.search(cur_line) is not None:
            full_line = full_line.replace(',\n', '\n')
            four_params = log_dict_four_params_regex.search(full_line)
            three_params = log_dict_three_params_regex.search(full_line)
            if four_params is not None:
                add_four_params(four_params)
            elif three_params is not None:
                add_three_params(three_params)
                a = 1
            full_line = ''
        else:
            full_line = full_line.replace('\n', '')


def get_param_name(param):
    if param == '1':
        return 'user'
    elif param == '2':
        return 'computer'
    elif param == '3':
        return 'application'
    elif param == '4':
        #return 'connection'
        return 'event'
    elif param == '5':
        return 'metadata'
    elif param == '6':
        return 'server'
    elif param == '7':
        return 'port'
    elif param == '8':
        return 'syncPort'
    else:
        return param


def add_predefined_params():

    log_dict['transactionStatus'] = {'N':'NotApplicable',
                                 'U':'Unfinished',
                                 'R':'RolledBack',
                                 'C':'Committed'}

    log_dict['level'] = {'I':'Information',
                             'E':'Error',
                             'W':'Warning',
                             'N':'Note'}
    

def prepare_log_dict():
    add_params_from_file()
    add_predefined_params()


##################################################################################
################## Чтение логов ##################################################
##################################################################################

def update_settings_file(int_file_name, full_line):
    
    setting_to_save = {
        'log_path': log_path,
        'level': level,
        'last_parsed_date': int_file_name,
        'last_parsed_message': full_line
    }
    
    settings_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "1C_event_log_parser_settings.json")
    with open(settings_path, 'w') as outfile:
        json.dump(setting_to_save, outfile)
        
        

def get_log_event_dict(log_event):

    
    log_event_dict = {
    
        'date' : log_event.group(1),
        'transactionStatus' : log_dict['transactionStatus'][log_event.group(2)],
        'transaction' : log_event.group(3),
        'user' : log_dict['user'][log_event.group(4)]['name'],
        'computer' : log_dict['computer'][log_event.group(5)],
        'application' : log_dict['application'][log_event.group(6)],
        'connection' : log_event.group(7),
        'event' : log_dict['event'][log_event.group(8)],
        'level' : log_dict['level'][log_event.group(9)],
        'comment' : log_event.group(10),
        'metadata' : log_dict['metadata'][log_event.group(11)]['name'],
        'data' : log_event.group(12),
        'dataPresentation' : log_event.group(13),
        'server' : log_dict['server'][log_event.group(14)],
        'port' : log_dict['port'][log_event.group(15)],
        'syncPort' : log_dict['syncPort'][log_event.group(16)],
        'session' : log_event.group(17)
        
    }
    
    return log_event_dict


def read_log_file(file_path, int_file_name, last_parsed_message):

    dict_file = open(file_path, encoding="utf8")
    
    # пропускаем первые три строки
    dict_file.readline()
    dict_file.readline()
    dict_file.readline()
    
    full_line = ''
    
    while True:

        cur_line = dict_file.readline()

        if not cur_line:
            break

        if next_event_regex.search(cur_line) is not None and full_line != '':

            #print(full_line)
            
            if last_parsed_message != '' and full_line != last_parsed_message:
                full_line = cur_line
                continue
            elif last_parsed_message != '' and full_line == last_parsed_message:
                last_parsed_message = ''
                full_line = cur_line
                continue
            
            log_event = log_event_regex.search(full_line)

            if log_event is not None:
                log_event_dict = get_log_event_dict(log_event)
                
                print(json.dumps(log_event_dict))
                
                # TODO: запись в файл выполняется очень долго
                # необоходимо исправить!!!
                update_settings_file(int_file_name, full_line)
            
                
            full_line = cur_line
            
            
        else:
            full_line = full_line + cur_line
            

def read_logs_from_files(last_parsed_message):

    folder = []

    for i in os.walk(log_path):
        folder.append(i)
        

    log_file_regex = re.compile(r'(\d{14})\.lgp')


    for address, dirs, files in folder:
        for file in files:
            file_path = os.path.join(address, file)
            log_file_descr = log_file_regex.search(file_path)
            
            if log_file_descr is not None:
                int_file_name = int(log_file_descr.group(1))
                
                if (last_parsed_date != '' and int_file_name == int(last_parsed_date)) or last_parsed_date == '':
                    read_log_file(file_path, int_file_name, last_parsed_message)
                elif last_parsed_date != '' and int_file_name > int(last_parsed_date):
                    last_parsed_message = ''
                    read_log_file(file_path, int_file_name, last_parsed_message)

##################################################################################
################## Вспомогательные функции #######################################
##################################################################################

def get_level_to_regex():
    if level == '2':
        level_to_regex = 'NWE'
    elif level == '3':
        level_to_regex = 'WE'
    elif level == '4':
        level_to_regex = 'E'
    else:
        level_to_regex = 'INWE'
    
    return level_to_regex
    

def get_date_to_regex(for_event):
    
    if for_event:
        if len(log_date) == 6:
            date_to_regex = log_date + '\d{8}'
        elif len(log_date) == 8:
            date_to_regex = log_date + '\d{6}'
        elif len(log_date) == 10:
            date_to_regex = log_date + '\d{4}'
        else:
            date_to_regex = '\d{14}'
            
    else:
        if len(log_date) == 6:
            date_to_regex = log_date + '\d{8}'
        elif len(log_date) == 8:
            date_to_regex = log_date + '\d{6}'
        elif len(log_date) == 10:
            date_to_regex = log_date[:8] + '\d{6}'
        else:
            date_to_regex = '\d{14}'
            
    return date_to_regex
        

##################################################################################

print(datetime.datetime.now())

settings = get_settings_from_file()

log_path = settings['log_path']
last_parsed_date = settings['last_parsed_date']
last_parsed_message = settings['last_parsed_message']
level = settings['level']


log_dict = {}


log_dict_four_params_regex = re.compile(r'\{(\d+),"?([^",]+)"?,"?([^",]*)"?,(\d+)\}')
log_dict_three_params_regex = re.compile(r'\{(\d+),"?([^",]+)"?,(\d+)\}')


prepare_log_dict()

print(datetime.datetime.now())
        
log_event_regex = re.compile(r'''\{
                             (\d{14}),([NURC]),\n                           # 20200316095741,N,
                             (\{[\w\d]+,[\w\d]+\}),                         # {0,0} или {24387918c7a90,9f1b} и т.п.
                             (\d+),(\d+),(\d+),(\d+),(\d+),([%s]),          # 3,4,2,2,6,I,
                             "((?:(?!(?:",\d+,\n\{))(?:.|\n))*)",           # комментарий в кавычках за которым следует ,\d+,\n\{
                             (\d+),\n
                             (\{"\w"(?:(?!(?:\},"))(?:.|\n))*\}),           # {"U"} или {"R",178:966d3ca82a232bfc11ea674703f56a8d} и т.п. за которым следует ,"
                             "((?:(?!(?:",\d+,\d+,\d+,\d+,\d+,\n))(?:.|\n))*)",         # представление объекта в кавычках, за которым следует 2,14,0,3,0, или чтото подобное
                             (\d+),(\d+),(\d+),(\d+),(\d+),\n               # 2,14,0,3,0,\n
                             \{\d\}\n\}
                             ''' % get_level_to_regex(), re.VERBOSE)


next_event_regex = re.compile(r'\d{14},[NURC],\n')


read_logs_from_files(last_parsed_message)

print(datetime.datetime.now())