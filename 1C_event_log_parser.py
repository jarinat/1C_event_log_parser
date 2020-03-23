#! python3
import re
import os
import json
import datetime
import argparse


##################################################################################
############# Получение параметров ###############################################
##################################################################################


def get_args():
    
    parser = argparse.ArgumentParser(description = 'Парсинг журнала регистрации 1С.\
                                                    Журнал должен быть в старом формате (lgp)\
                                                    и разбит по дням.')

    parser.add_argument(
        '--log_path',
        type=str,
        default=os.getcwd(),
        help='Каталог ресположения журнала регистрации.\
            По-умолчанию - текущий рабочий каталог.')


    parser.add_argument(
        '--log_date',
        type=str,
        default='',
        help='Регулярное выражение - логи за какую дату анализировать.\
            Формат даты - YYYYMMDDHH\
            Допустимые форматы - YYYYMM, YYYYMMDD, YYYYMMDDHH.\
            По-умолчанию - пустая дата')
            
    parser.add_argument(
        '--periodic',
        type=str,
        default='False',
        help='Периодическая обработка журанала.\
            Если True, тогда создается файл настроек,\
            в котором хранится дата последнего запуска скрипта\
            и прочие настройки.\
            По-умолчанию - False')
            
    parser.add_argument(
        '--level',
        type=str,
        default='1',
        help='Минимальный уровень обрабатываемых событий.\
            1 - все события,\
            2 - Замечания, Предупреждения, Ошибки,\
            3 - Предупреждения, Ошибки,\
            4 - Ошибки.\
            в котором хранится дата последнего запуска скрипта\
            и прочие настройки.\
            По-умолчанию - 1')


    return parser.parse_args()


def update_params_from_file(log_path, log_date, level):
    
    settings_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "1C_JR_analyze_settings.json")
    
    if os.path.exists(settings_path):
        with open(settings_path) as json_file:
            data = json.load(json_file)
            
            if 'log_path' in data:
                log_path = data['log_path']
                
            if 'last_parsing_date' in data:
                last_parsing_date = data['last_parsing_date']
                
            if 'level' in data:
                level = data['level']
                
                
    

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


def read_log_file(file_path):

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
            log_event = log_event_regex.search(full_line)

            if log_event is not None:
                log_event_dict = get_log_event_dict(log_event)
                print(json.dumps(log_event_dict))
            
                
            full_line = cur_line
            
            
        else:
            full_line = full_line + cur_line
            

def read_logs_from_files():

    folder = []

    for i in os.walk(log_path):
        folder.append(i)
        

    log_file_regex = re.compile(r'%s\.lgp' % get_date_to_regex(False))


    for address, dirs, files in folder:
        for file in files:
            file_path = os.path.join(address, file)
            if log_file_regex.search(file_path) is not None:
                read_log_file(file_path)

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

#args = get_args()


args = get_args_from_file()


log_path = args.log_path
log_date = args.log_date
level = args.level
if args.periodic.upper() == 'TRUE':
    update_params_from_file(log_path, log_date, level)


log_dict = {}


log_dict_four_params_regex = re.compile(r'\{(\d+),"?([^",]+)"?,"?([^",]*)"?,(\d+)\}')
log_dict_three_params_regex = re.compile(r'\{(\d+),"?([^",]+)"?,(\d+)\}')


prepare_log_dict()
        
log_event_regex = re.compile(r'''\{
                             (%s),([NURC]),\n                           # 20200316095741,N,
                             (\{[\w\d]+,[\w\d]+\}),                         # {0,0} или {24387918c7a90,9f1b} и т.п.
                             (\d+),(\d+),(\d+),(\d+),(\d+),([%s]),        # 3,4,2,2,6,I,
                             "((?:(?!(?:",\d+,\n\{))(?:.|\n))*)",           # комментарий в кавычках за которым следует ,\d+,\n\{
                             (\d+),\n
                             (\{"\w"(?:(?!(?:\},"))(?:.|\n))*\}),           # {"U"} или {"R",178:966d3ca82a232bfc11ea674703f56a8d} и т.п. за которым следует ,"
                             "((?:(?!(?:",\d+,\d+,\d+,\d+,\d+,\n))(?:.|\n))*)",         # представление объекта в кавычках, за которым следует 2,14,0,3,0, или чтото подобное
                             (\d+),(\d+),(\d+),(\d+),(\d+),\n               # 2,14,0,3,0,\n
                             \{\d\}\n\}
                             ''' % (get_date_to_regex(True), get_level_to_regex()), re.VERBOSE)


next_event_regex = re.compile(r'\d{12},[NURC],\n')


#read_logs_from_files()











