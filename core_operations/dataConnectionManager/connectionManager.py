from . import connectionsConfig

class DBsConnections(object):
    """ Handling the SQL and NoSQL DBMSs Connections "mongodb and mysql (sphinx)" """
    def __init__(self):
        ####Shpinx Config####
        self.shpinxHost     = connectionsConfig.SPHINX_HOST
        self.shpinxPort     = int(connectionsConfig.SPHINX_PORT)

        ####Mongodb Config####
        self.mongodbHost        = connectionsConfig.MONGO_HOST
        self.mongodbClient      = connectionsConfig.MONGO_CLIENT
        self.mongodbPort        = connectionsConfig.MONGO_PORT
        self.mongodbPool        = connectionsConfig.MONGO_POOL
        self.mongodbCollection  = connectionsConfig.MONGO_COLLECTION
        self.mongodbService     = connectionsConfig.MONGO_SERVICE

        ####Postgres Config####
        self.pgName         = connectionsConfig.PG_CORE_DATABASE_NAME
        self.pgHost         = connectionsConfig.PG_CORE_HOST
        self.pgPort         = connectionsConfig.PG_CORE_PORT
        self.pgUsername     = connectionsConfig.PG_CORE_USERNAME
        self.pgPassword     = connectionsConfig.PG_CORE_PASSWORD

    def sphinxReadExe(self,stmt,service):
        import MySQLdb
        if service == 'baytSkillsAnalyser':
            host    = self.shpinxHost
            port    = self.shpinxPort
        else:
            return
        try:
            spx_db = MySQLdb.connect(host=host,port=port,charset='utf8')
            cur = spx_db.cursor()
            cur.execute(stmt)
            res = cur.fetchall()
            spx_db.close()
            return res
        except Exception as e:
            print('Failed to connect Shpinx\n', e)

    def pgCoreReadExe(self,stmt,service):
        import psycopg2
        if service == 'baytSkillsAnalyser':
            name        =   self.pgName
            user        =   self.pgUsername
            host        =   self.pgHost
            password    =   self.pgPassword
        else:
            return
        try:
            pg_db = psycopg2.connect(dbname=name, user=user, host=host, password=password)
            cur = pg_db.cursor()
            cur.execute(stmt)
            res = cur.fetchall()
            pg_db.close()
            return res
        except Exception as e:
            print('Failed to connect postgres\n', e)

    def mongodbConnectionInfo(self):
        from pymongo import MongoClient
        client      = self.mongodbClient
        host        = self.mongodbHost
        port        = self.mongodbPort
        pool        = self.mongodbPool
        collection  = self.mongodbCollection
        client      = MongoClient(client+'://'+host+':'+port+'/')
        return [client,pool,collection]

dbCon = DBsConnections()
