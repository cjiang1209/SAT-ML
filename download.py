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
	'LIMIT 2'
print(query)

cursor = cnx.cursor()
cursor.execute(query, (solver_config_id, result_id))

jobs = []
for row in cursor:
    jobs.append(row)

cursor.close()

root = 'instances'
# Clean directory
if not os.path.isdir(root):
    os.makedirs(root)

# [instance_id, instance_name, instance_md5, job_id]
for job in jobs:
    print(job)

    instance_id, name, md5, job_id = job
    
    print('Reading {}...'.format(name))
    query = 'SELECT instance FROM Instances ' + \
	    'WHERE idInstance = %s'
    cursor = cnx.cursor()
    cursor.execute(query, (instance_id,))
    row = cursor.fetchone()
    assert row is not None
    
    dir = os.path.join(root, md5)
    if not os.path.isdir(dir):
        os.makedirs(dir)
    
    if is_lzma(row[0]):
        with open(os.path.join(dir, name + '.lzma'), 'wb') as file:
            file.write(row[0][4:])
        print("Decompressing {}...".format(name + '.lzma'))
        with lzma.open(os.path.join(dir, name + '.lzma'), 'rb') as lzma_file:
            with open(os.path.join(dir, name), 'wb') as file:
                for row in lzma_file:
                    file.write(row)
        os.remove(os.path.join(dir, name + '.lzma'))
    else:
        with open(os.path.join(dir, name), 'wb') as file:
            file.write(row[0])
    cursor.close()

    print('Reading the result...')
    query = 'SELECT solverOutput FROM ExperimentResultsOutput ' + \
	    'WHERE ExperimentResults_idJob = %s'
    cursor = cnx.cursor()
    cursor.execute(query, (job_id,))
    row = cursor.fetchone()
    assert row is not None
    with open(os.path.join(dir, name + '.result'), 'wb') as file:
        file.write(row[0])
    cursor.close()

    # Extract assignment
    print('Extracting the assignment...')
    with open(os.path.join(dir, name + '.result'), 'r') as result_file:
        with open(os.path.join(dir, name + '.assign'), 'w') as assign_file:
            for line in result_file:
                if line.startswith('v '):
                    assign_file.write(line[len('v '):])

cnx.close()