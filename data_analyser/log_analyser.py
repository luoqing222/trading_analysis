__author__ = 'Qing'


class LogAnalyser:
    def __init__(self, file_location, file_name, keyword):
        self.file_location = file_location
        self.file_name = file_name
        self.keyword = keyword

    def findKeyWord(self):
        '''
        :return:function to check if hte key word in the log file or not
        '''
        fname= self.file_location+"/"+self.file_name
        with open(fname) as f:
            for content in f:
                for word in self.keyword:
                    if content.find(word) != -1:
                        return True

        return False




