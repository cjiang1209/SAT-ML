import mysql.connector
import lzma
import os
import configparser

def is_lzma(buf):
    # The first four binaries are 'LZMA'
    return chr(buf[0]) == 'L' and chr(buf[1]) == 'Z' and chr(buf[2]) == 'M' and chr(buf[3]) == 'A'

# Lingeling bbc
solver_config_id = 75
# SAT
result_id = 11

config = configparser.ConfigParser()
config.read('config.ini')
default = config[config.default_section]

cnx = mysql.connector.connect(host=default['host'], user=default['user'], password=default['password'], database=default['database'])

query = 'SELECT Inst.idInstance, Inst.name, Inst.md5, Res.idJob ' + \
    'FROM ExperimentResults AS Res ' + \
    'INNER JOIN Instances AS Inst ON Res.Instances_idInstance = Inst.idInstance ' + \
    'WHERE Res.SolverConfig_idSolverConfig = %s ' + \
	'AND Res.resultCode = %s ' + \
	'LIMIT 5'
print(query)

cursor = cnx.cursor()
cursor.execute(query, (solver_config_id, result_id))

jobs = []
for row in cursor:
    jobs.append(row)

cursor.close()

# [instance_id, instance_name, instance_md5, job_id]
for job in jobs:
    print(job)

    print('Reading {}...'.format(job[1]))
    query = 'SELECT instance FROM Instances ' + \
	    'WHERE idInstance = %s'
    cursor = cnx.cursor()
    cursor.execute(query, (job[0],))
    row = cursor.fetchone()
    assert row is not None
    if is_lzma(row[0]):
        with open(job[1] + '.lzma', 'wb') as file:
            file.write(row[0][4:])
        print("Decompressing {}...".format(job[1] + '.lzma'))
        with lzma.open(job[1] + '.lzma', 'rb') as lzma_file:
            with open(job[1], 'wb') as file:
                for row in lzma_file:
                    file.write(row)
        os.remove(job[1] + '.lzma')
    else:
        with open(job[1], 'wb') as file:
            file.write(row[0])
    cursor.close()

    print('Reading the result of {}...'.format(job[1]))
    query = 'SELECT solverOutput FROM ExperimentResultsOutput ' + \
	    'WHERE ExperimentResults_idJob = %s'
    cursor = cnx.cursor()
    cursor.execute(query, (job[3],))
    row = cursor.fetchone()
    assert row is not None
    with open(job[1] + '.result', 'wb') as file:
        file.write(row[0])
    cursor.close()

cnx.close()