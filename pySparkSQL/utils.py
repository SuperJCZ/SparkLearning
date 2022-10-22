import os.path

from pyspark.sql import SparkSession


class FileReader():
    def __int__(self,path=''):
        pass

    @property
    def remoteTestFilePath(self):
        return "/home/sam/pysparkTestFiles/sql/"

    def read_txt(self,spark:SparkSession,file_name:str):
        return spark.read.\
            format("txt").\
            load(os.path.join(self.remoteTestFilePath,file_name))

    def read_csv(self,spark:SparkSession,file_name:str,**options):
        return spark.read.format("csv"). \
            options(**options). \
            load(self.remoteTestFilePath + file_name)


