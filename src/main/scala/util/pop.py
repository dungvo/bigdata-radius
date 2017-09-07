def  read_pop(d):
    files = glob.glob(os.path.join(d + 'datapay_*.txt'))
    pop = pd.DataFrame()
    for file in files:
        if 'pon' in file:
            a = pd.read_table(file, index_col=None, header=None, sep='\t', usecols=[1,2,4,5,9,18,19,21,22,23])
            a.columns = ['contract', 'name', 'status', 'splitter', 'date', 'ip', 'host', 'module', 'index', 'mac']
            a['type'] = 'OLT'
        else:
            a = pd.read_table(file, index_col=None, header=None, sep='\t', usecols=[1,2,4,5,8,17,18,21])
            a.columns = ['contract', 'name', 'status', 'splitter', 'date', 'ip', 'host', 'mac']
            a['module'] = np.nan
            a['index'] = np.nan
            a['type'] = 'DSLAM'
        pop = pop.append(a, ignore_index=True)
    pop = pop.drop_duplicates()
    pop['contract'] = pop['contract'].str.upper()
    pop['mac'] = pop['mac'].str.upper()
    pop['splitter'] = pop['splitter'].str.upper()
    pop = pop[pop.contract.str.contains('\s') == False]
    pop['name'] = pop['name'].str.upper()
    pop['date'] = pd.to_datetime(pop['date'], errors='coerce')
    pop.ix[pop['status'] == 'Binh thuong', 'status'] = 'Active'
    pop.ix[pop['status'] == 'Ngung vi ly do thanh toan', 'status'] = 'Active'
    pop.ix[pop['status'] != 'Active', 'status'] = 'Drop'
    
    pop = pop[['contract','ip','host','module','index','type','name','status','mac','splitter']]
    pop = pop.dropna(subset=['ip','host'])
    pop['module'] = pop['module'].fillna(-1).astype(int).astype(str).replace('-1','')
    pop['index'] = pop['index'].fillna(-1).astype(int).astype(str).replace('-1','')
    return pop


def connection_v1():
    try:
        
        conn = psycopg2.connect("dbname='dwh_noc' user='dwh_noc' host='172.27.11.153' password='bigdata'")
        conn = psycopg2.connect(st)
    except:
        print "I am unable to connect to the database"
    return conn



def connection_v2():
    try:
        
        engine = sqlalchemy.create_engine("postgresql+psycopg2://dwh_noc:bigdata@172.27.11.153:5432/dwh_noc"
                                        , isolation_level='READ COMMITTED')
        engine = sqlalchemy.create_engine(st, isolation_level='READ COMMITTED')
    except:
        print "I am unable to connect to the database"
    return engine


def insert_pop(month):
    try:
        pop = read_pop(month)
        
        conn = connection_v1()
        cur = conn.cursor()
        sql = """TRUNCATE TABLE pop ;"""
        cur.execute(sql)
        conn.commit()
        
        conn = connection_v2()
        t = datetime.now()
        pop.to_sql('pop', conn, if_exists='append', index=False)
        t = (datetime.now() - t).total_seconds()
        conn.dispose()
        write_log('Insert table POP successfully: ' + str(t) + 's, ' + str(len(pop)) + ' rows')
    except:
        write_log("I am unable to insert POP to the database") 



if __name__ == '__main__'
    insert_pop("/hfasdfasdfasdf")




    