import os
import random

from auxiliary import HEADER, BLOCK, FOOTER


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
    list_name = 'bucket_' + str( i % number_of_buckets )
    pop = globals()[list_name].pop(0)
    globals()[list_name] = globals()[list_name] + [pop]
    replica_list = globals()[list_name]
    sufix = '' if(i == number_of_partitions - 1 ) else ','
    f.write(BLOCK.replace('<ITERATOR>', str(i)).replace('<TOPIC_NAME>', topic_name).replace('<REPLICA_LIST>', str(replica_list)) + sufix + "\n")
  f.write(FOOTER)
  f.close()
  print("Partition reassignment file created.")

def compute_replica_buckets(number_of_racks, number_of_buckets):
  for k in range(number_of_buckets):
      globals()['bucket_' + str(k)] = [k + x * number_of_buckets for x in range(number_of_racks)]

create_partitions_file(number_of_racks=4, number_of_machines=12, topic_name='test_topic', number_of_partitions=16)
