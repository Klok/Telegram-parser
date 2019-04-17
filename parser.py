import os
import pandas as pd
from html.parser import HTMLParser

class TelegramHTMLParser(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = pd.DataFrame({'id': '', 'name': '', 'text': '', 'time': ''}, index=[0])
        self.line_class = ''
        self.restart_flag = False
        self.id_flag = False
        self.name_flag = False
        self.text_flag = False
        self.time_flag = False
    def reinitialize(self):
        self.fed = pd.DataFrame({'id': '', 'name': self.fed.iloc[0]['name'], 'text': '', 'time': ''}, index=[0])
        self.restart_flag = False
    def handle_data(self, d):
        d = d.strip()
        if d != '':
            if self.name_flag:
                self.fed.iloc[0]['name'] = d
                self.name_flag = False
            elif self.text_flag:
                self.fed.iloc[0]['text'] = d
                self.text_flag = False
                self.restart_flag = True
    def get_data(self):
        if (not self.id_flag)&(not self.name_flag)&(not self.text_flag)&(not self.time_flag)&(self.restart_flag):
            output = self.fed
            self.reinitialize()
            return output
        else:
            return
    def handle_starttag(self, tag, attrs):
        for attr in attrs:
            if (attr == ('class', 'message default clearfix'))|(attr == ('class', 'message default clearfix joined')):
                self.id_flag = True
                continue
            elif attr == ('class', 'from_name'):
                self.name_flag = True
            elif attr == ('class', 'text'):
                self.text_flag = True
            elif attr == ('class', 'pull_right date details'):
                self.time_flag = True

            if (self.id_flag)&(attr[1].find('message') != -1):
                self.fed.iloc[0]['id'] = attr[1].replace('message', '')
                self.id_flag = False
            elif (self.time_flag)&(attr[0] == 'title'):
                self.fed.iloc[0]['time'] = attr[1]
                self.time_flag = False

def load_file(path_to_data, inp_filename):
    with open(os.path.join(path_to_data, inp_filename), encoding='utf-8') as inp_html_file:
        parser = TelegramHTMLParser()
        result = pd.DataFrame()
        for line in inp_html_file:
            parser.feed(line)
            data = parser.get_data()
            try:
                if data.iloc[0]['text'] != '':
                    result = pd.concat([result, data])
            except:
                continue
        return result.set_index('id')

def parse_files(path_to_data):
    with open(os.path.join(path_to_data,'dataset.txt'), 'w', encoding='utf-8') as dataset:
        file_exist = True
        file_ids = []
        for (dirpath, dirnames, filenames) in os.walk(path_to_data):
            for file_name in filenames:
                if (file_name.endswith('.html'))&(file_name.startswith('messages')):
                    file_ids.append(file_name)
        for id in file_ids:
            try:
                result = parser.load_file(path_to_data, id)
                result.to_csv(dataset, mode='a')
            except:
                print('Some nasty error occured in parse_files loop')
