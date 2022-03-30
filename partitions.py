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
  replica_buckets = [[k + x * number_of_buckets for x in range(number_of_racks)] for k in range(number_of_buckets)]
  f.write(HEADER + "\n")
  for i in range(number_of_partitions):
    bucket_index = i % number_of_buckets
    pop = replica_buckets[bucket_index].pop(0)
    replica_buckets[bucket_index] += [pop]
    sufix = '' if(i == number_of_partitions - 1 ) else ','
    f.write(BLOCK.replace('<ITERATOR>', str(i)).replace('<TOPIC_NAME>', topic_name).replace('<REPLICA_LIST>', str(replica_buckets[bucket_index])) + sufix + "\n")
  f.write(FOOTER)
  f.close()
  print("Partition reassignment file created.")


create_partitions_file( number_of_racks=4, number_of_machines=8, topic_name='test_topic', number_of_partitions=16)