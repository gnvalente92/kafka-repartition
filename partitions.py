import os
import random

# ==================================================
# AUX VARIABLES
# ==================================================
HEADER = '{"version":1, "partitions":['
BLOCK = '{"topic":"<TOPIC_NAME>", "partition":<ITERATOR>, "replicas":<REPLICA_LIST>}'
FOOTER = ']}'
# ==================================================

def create_partitions_file (number_of_racks, number_of_machines, topic_name, number_of_partitions):
  print('Partition reassignment json file creator script.')
  output_file_name = topic_name + '_r' + str(number_of_racks) + '_m' + str(number_of_machines) + '_p' + str(number_of_partitions) + '.json'
  print("Partition reassignment file name will be " + output_file_name + '.')
  if os.path.exists(output_file_name):
    os.remove(output_file_name)
  f = open(output_file_name, "a")
  number_of_buckets = int(number_of_machines / number_of_racks)
  compute_replica_buckets(number_of_racks, number_of_buckets)
  f.write(HEADER + "\n")
  for i in range(number_of_partitions):
    pop = globals()['bucket_' + str( i % number_of_buckets )].pop(0)
    globals()['bucket_' + str( i % number_of_buckets )] = globals()['bucket_' + str( i % number_of_buckets )] + [pop]
    replica_list = globals()['bucket_' + str( i % number_of_buckets )]
    if ( i == number_of_partitions - 1 ):
        f.write(BLOCK.replace('<ITERATOR>', str(i)).replace('<TOPIC_NAME>', topic_name).replace('<REPLICA_LIST>', str(replica_list)) + "\n")
    else:
        f.write(BLOCK.replace('<ITERATOR>', str(i)).replace('<TOPIC_NAME>', topic_name).replace('<REPLICA_LIST>', str(replica_list)) + ',' + "\n")
  f.write(FOOTER)
  f.close()
  print("Partition reassignment file created.")

def compute_replica_buckets(number_of_racks, number_of_buckets):
  for k in range(number_of_buckets):
      globals()['bucket_' + str(k)] = [] 
      for j in range(number_of_racks):
        globals()['bucket_' + str(k)] += [k + j * number_of_buckets]

create_partitions_file(number_of_racks=4, number_of_machines=12, topic_name='test_topic', number_of_partitions=16)
