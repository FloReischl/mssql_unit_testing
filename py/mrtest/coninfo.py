

class ConInfo:
    def __init__(self, server: str, database: str=None, user: str=None, password:str =None) -> None:
        self.server = server
        self.database = database
        self.user = user
        self.password = password
    
    # 'DRIVER={SQL Server};SERVER=.;DATABASE=testdb;User Id=flo_test;Password=test-123;'
    def odbc_cnstr(self) -> str:
        cnstr = "DRIVER={SQL Server};SERVER=" + self.server

        if self.database:
            cnstr += ";DATABASE=" + self.database + ";"
        
        if self.user:
            cnstr += f"User Id={self.user};Password={self.password};"
        else:
            cnstr += "Trusted_Connection=True;"

        return cnstr

    #"mssql+pyodbc://sqlaircyber2.munichre.com:1433/dev_sandbox?driver=ODBC+Driver+17+for+SQL+Server"
    def alchemy_url(self) -> str:
        url = "mssql+pyodbc://"

        if self.user: 
            url += self.user + ":" + self.password
        
        url += self.server

        if self.database:
            url += "/" + self.database
        
        url += "?driver=ODBC+Driver+17+for+SQL+Server"

        return url
