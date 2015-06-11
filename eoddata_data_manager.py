__author__ = 'qingluo'

import configparser
import datetime
import re
import os
from ftplib import FTP
import shutil

class EodDataDataManager:
    def __init__(self):
        self.config_file = "option_data_management_setting.ini"
        pass

    def decompose_option_contract(self, contract_name):
        result=[]
        option_name = re.compile("^[A-Z]+\.*[A-Z]+\d{6}[CP]\d{8}")
        if not option_name.match(contract_name):
            return None
        underlying_stock = re.sub("\d{6}[CP]\d{8}", "", contract_name)
        contract_name = re.sub("^[A-Z]+\.*[A-Z]+","", contract_name)
        expiration_date = re.sub("[CP]\d{8}", "", contract_name)
        contract_name = re.sub("^\d{6}", "", contract_name)
        option_type= re.sub("\d{8}", "", contract_name)
        contract_name = re.sub("^[CP]", "", contract_name)
        strike_price = contract_name

        #print "underlying_stock is ", underlying_stock
        #print "expiration_date is ", expiration_date
        #print "option_type is ", option_type
        #print "strike_price is ", strike_price
        return [underlying_stock,expiration_date,option_type,strike_price]


    #this function convert the eod option data .txt format into .csv format
    def option_txt_to_csv(self, src_folder, src_file, des_folder, des_file):
        src_file_name = src_folder + "/" + src_file
        des_file_name = des_folder + "/" + des_file

        #if the file doesn't exist
        if not os.path.exists(src_file_name):
            print src_file_name + " does not exists at all!"
            return

        #print des_file_name
        output = open(des_file_name,"w")
        with open(src_file_name) as fp:
            for line in fp:
                [contract_name, date, open_price, high, low, close, volume, open_interest] = line.split(",")
                temp = self.decompose_option_contract(contract_name)
                if temp is not None:
                    underlying_stock = temp[0]
                    expiration_date = temp[1]
                    option_type = temp[2]
                    strike_price = str(int(temp[3]))
                    output.write(underlying_stock + "," + expiration_date + "," + option_type+"," + strike_price+",")
                    output.write(date+ "," + open_price + "," + high + "," + low + "," + close + "," + volume + "," + open_interest)

    #this function to copy the equity data .txt format into .csv format
    def copy_txt_to_csv(self,src_folder, src_file, des_folder, des_file):
        src_file_name = src_folder + "/" + src_file
        des_file_name = des_folder + "/" + des_file
        #if the file doesn't exist
        if not os.path.exists(src_file_name):
            print src_file_name + " does not exists at all!"
            return

        shutil.copyfile(src_file_name, des_file_name)

    def daily_run(self):
        Config = configparser.ConfigParser()
        Config.read(self.config_file)

        #only download the data that is on running time day
        running_time=datetime.datetime.now().strftime("%Y%m%d")

        host = Config.get("eod","host")
        user = Config.get("eod","user")
        password = Config.get("eod","passwd")
        des_folder = Config.get("csv", "temp_folder")+"/"+Config.get("eod", "data_folder")
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)

        ftp = FTP(host=host, user=user, passwd = password)
        files = ftp.nlst()
        file_pattern = re.compile('[a-zA-Z]+_\d{8}.txt\Z')
        for f in files:
            if file_pattern.match(f):
                file_date = f.replace('.txt', "")
                file_date = re.sub('[a-zA-Z]+_','',file_date)
                if file_date == running_time:
                    the_folder= des_folder+"/"+file_date
                    if not os.path.exists(the_folder):
                        os.makedirs(the_folder)
                    des_file_name=the_folder+"/"+f
                    print "downloading "+f+" to "+the_folder
                    ftp.retrbinary('RETR '+f, open(des_file_name, 'wb').write)

        ftp.close()

        #process the txt files after it is downloaded
        src_folder = des_folder+"/"+running_time
        des_folder = Config.get("csv", "data_folder")+"/"+Config.get("eod", "data_folder")
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)

        des_folder = des_folder+"/"+running_time
        #print des_folder
        if not os.path.exists(des_folder):
            os.makedirs(des_folder)

        self.option_txt_to_csv(src_folder,"OPRA_"+running_time+".txt",des_folder, "OPRA_"+running_time+".csv")
        self.copy_txt_to_csv(src_folder,"NYSE_"+running_time+".txt",des_folder, "NYSE_"+running_time+".csv")
        self.copy_txt_to_csv(src_folder,"NASDAQ_"+running_time+".txt",des_folder, "NASDAQ_"+running_time+".csv")


