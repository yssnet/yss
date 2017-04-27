#-*- coding:utf-8 -*-

import re
import time
import os
from multiprocessing import Process as pro

class ctr_to_csv(object):
    '''ctr to csv'''

    def __init__(self):
        self.p1 = re.compile(r'^(?!.*[{|}|[])')
        self.csv_list = []
        self.temp_list = []
        self.temp_file_list = []
        self.file_list = []
        self.m = 0
        self.n = 0
        self.h = 0
        self.recordtype_tag = False
        self.L3_tag = False
        self.RRC_tag = False
        self.eventId_tag = False


    def rule(self,line):
        '''规则筛选'''

        if '[' in line and ']' in line and 'LTEEvent' in line:
            self.n = 0
            self.h = 0
            self.temp_list = []
            #self.file_list = []
            self.temp_file_list = []
            self.recordtype_tag = False
            self.L3_tag = False
            self.RRC_tag = False
            self.eventId_tag = False
            #self.temp_list.append(line.split(']')[0].strip('['))

        if '{' in line:
            self.n += 1
            self.m += 1
            self.h += 1
            if self.n == 2 and 'RRC {' not in line:
                self.temp_list.append(line.split('{')[0].strip())
            #RRC tag
            #非特定事件判断L3MESSAGE
            if 'RRC {' in line:
                RRC_tag = True

        if re.match(self.p1,line):
            #非特定事件判断L3MESSAGE
            if 'L3MESSAGE' in line:
                self.L3_tag = True
            if self.RRC_tag and self.L3_tag:
                RRC_tag = 'pass'
            if self.n >= 2  and self.h <= 2:
                line_var = line.split()[-1].strip(',').rstrip(')').lstrip('(')
                line_var1 = ' '.join(line.split()[1:])
                if line_var == 'unavailable':
                    self.temp_list.append('-1')
                elif "'H" in line_var1:
                    self.temp_list.append(line_var1)
                else:
                    self.temp_list.append(line_var)
                #self.temp_file_list.append(line.split()[0])
            if 'eventId' in line:
                pass
                #self.temp_list.append(' '.join(line.split()[1:]).strip(','))
                #self.temp_file_list.append(' '.join(line.split()[1:]).strip(','))

            #判断recordType 0 和 3
            try:
                if line.split()[0] == 'recordType':
                    if str(line.split()[1].strip(',')) == '3' \
                            or str(line.split()[1].strip(',')) == '0':
                        self.recordtype_tag = True
            except:
                print line

            #特定事件(eventId 11)+ L3:RSRPResult|L3:RSRQResult
            if 'eventId' in line and str(line.split()[1].strip(',')) == '11':
                self.eventId_tag = True

        if '}' in line:
            self.n -= 1

        if self.n == 0 and self.m >0:
            self.m = 0
            if self.recordtype_tag:
                pass
            elif self.eventId_tag:
                self.temp_list.append('L3:RSRPResult')
                #self.temp_list.append('L3:RSRQResult')
                self.csv_list.append(self.temp_list)
            else:
                self.csv_list.append(self.temp_list)
                #self.file_list.append(self.temp_file_list)


class process(object):
    '''生成数据'''

    def __init__(self):
        self.data_files = []

    def get_data_file(self,data_dir):
        for d_dir in data_dir.split(','):
            for root,dirs,files in os.walk(d_dir):
                for i in files:
                    self.data_files.append(os.path.join(root,i))
        return self.data_files

    def create_file(self,file_list,csv_file_dir,data_file_name):
        '''
        :需要创建的cvs文件列表 file_list: 
        :cvs文件目录 cvs_file_dir: 
        :eventId data_file_name: 
        '''
        if not os.path.exists(csv_file_dir):
            self.data_files(os.mkdir(csv_file_dir))

        for file in file_list:
            eventid = file[0]
            csv_title = file[1:]
            csv_title = '|'.join(csv_title)
            with open(csv_file_dir + '/' + data_file_name + '_' + str(eventid) + '.csv','w') as f:
                f.write(csv_title + '\n')

    def write_csv_data(self,csv_list,csv_file_dir,data_file_name):
        for ev  in csv_list:
            ev_name = ev[0]
            ev_data =  ev[1:]
            ev_data = '|'.join(ev_data)
            #if os.path.exists():
            with open(csv_file_dir + '/' + ev_name +'_' + data_file_name + '_' \
                              + str(time.strftime('%Y%m%d%H%M',time.localtime(time.time()))) \
                              + '_00001' + '.txt','a') as f:
                f.write(ev_data + '\n')


if __name__ == '__main__':
    #元数据目录，如果有多个使用","分隔
    data_dir='/home/ss/PycharmProjects/eea/CTR'
    #csv文件生成的目录
    csv_file_dir='/home/ss/PycharmProjects/eea/CSV'
    csv = ctr_to_csv()
    pr = process()
    files = pr.get_data_file(data_dir)
    for file in files:
        data_file_name = ''.join(file.split('/')[-1][:14].lstrip('A').split('.'))
        with open(file,'r') as f:
            for line in f:
                #csv.rule(line)
                p = pro(target=csv.rule, args=(line,))
                p.start()
                p.join()
            pr.write_csv_data(csv.csv_list,csv_file_dir,data_file_name)

