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
        self.href_flag = False
        self.integrity_flag = (not self.id_flag)&(not self.name_flag)&(not self.text_flag)&(not self.time_flag)&(not self.href_flag)
        self.forwarded_flag = False
    def reinitialize(self):
        # function triggers after all chunk data processed
        self.reset()
        self.fed = pd.DataFrame({'id': '', 'name': self.fed.iloc[0]['name'], 'text': '', 'time': ''}, index=[0])
        self.restart_flag = False
        self.forwarded_flag = False
    def get_data(self):
        # function to trigger for each line read
        # normal message condition
        if (self.integrity_flag)&(not self.forwarded_flag)&(self.restart_flag):
            output = self.fed
            self.reinitialize()
            return output
        # forwarded message condition
        elif (self.integrity_flag)&(self.forwarded_flag)&(self.restart_flag):
            self.reinitialize()
            return
        # not ready to start looking for the new chunk condition
        else:
            return
    def handle_data(self, d):
        # function triggers on data found between tags
        d = d.strip()
        if d != '':
            if self.name_flag:
                self.fed.iloc[0]['name'] = d
                self.name_flag = False
            elif self.text_flag:
                if self.href_flag:
                    self.fed.iloc[0]['text'] = '{} {}'.format(self.fed.iloc[0]['text'], d)
                    self.href_flag = False
                else:
                    self.fed.iloc[0]['text'] = '{} {}'.format(self.fed.iloc[0]['text'], d)
    def handle_starttag(self, tag, attrs):
        # function to parse tag information
        if (tag == 'a')&(self.text_flag):
            self.href_flag = True
        for attr in attrs:
            # check for specific data indicators in tags
            if (attr == ('class', 'message default clearfix'))|(attr == ('class', 'message default clearfix joined')):
                self.id_flag = True
                # this attribute set still contains 'message' part so need to skip turn
                continue
            elif attr == ('class', 'from_name'):
                self.name_flag = True
            elif attr == ('class', 'text'):
                self.text_flag = True
            elif attr == ('class', 'pull_right date details'):
                self.time_flag = True
            elif attr == ('class', 'forwarded body'):
                self.forwarded_flag = True
            # get useful information from tags
            if (self.id_flag)&(attr[1].find('message') != -1):
                self.fed.iloc[0]['id'] = attr[1].replace('message', '')
                self.id_flag = False
            elif (self.time_flag)&(attr[0] == 'title'):
                self.fed.iloc[0]['time'] = attr[1]
                self.time_flag = False
    def handle_endtag(self, tag):
        if tag == 'div':
            if self.text_flag:
                self.fed.iloc[0]['text'] = self.fed.iloc[0]['text'].strip()
                self.text_flag = False
                self.href_flag = False
                self.restart_flag = True

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
            except(AttributeError):
                continue
        return result

def parse_files(path_to_data, csv_name):
    with open(os.path.join(path_to_data, csv_name), 'w', encoding='utf-8') as dataset:
        file_exist = True
        file_ids = []
        for (dirpath, dirnames, filenames) in os.walk(path_to_data):
            for file_name in filenames:
                if (file_name.endswith('.html'))&(file_name.startswith('messages')):
                    file_ids.append(file_name)
        for id in file_ids:
            try:
                result = load_file(path_to_data, id)
                result.to_csv(dataset, sep='\t', index=False, mode='a', header=False)
            except():
                print('Some nasty error occured in parse_files loop')
