import yaml
from multiprocessing import Process
import sqlalchemy



db_cred_file = '../Cred/db_cred.yaml'


class AWSDBConnector:

    """
    This implements a secured connection to AWS database
    
    """

    def __init__(self):
        
        self.cred = self.read_db_creds(db_cred_file)
        self.HOST = self.cred['HOST']
        self.USER = self.cred['USER']
        self.PASSWORD = self.cred['PASSWORD']
        self.DATABASE = self.cred['DATABASE']
        self.PORT = self.cred['PORT']

    def read_db_creds(self, file):
        """
        This method reads the database credentials from a file and returns a dict of the credentials
        
        Args:
        -----------------------------------------------
        file (str) : The yaml file name containing the database credentials
        
        Returns:
        -------------------------------------------------
        cred (dict) : The dictionary object of the database credentials
        """

        with open(file, 'r') as my_file:
            cred = yaml.safe_load(my_file)
        return cred
        
    def create_db_connector(self):
        """
        This method creates the engine used to connect to the database

        Returns
        --------------------------------------------------------
        engine : sqlalchemy engine
        """


        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine



