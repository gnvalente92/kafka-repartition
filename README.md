# kafka-repartition

## Requirements
Just requires `Python`. I am using `3.9.10`, but it should work with any modern version of `Python`.

## How to use
1. Input the correct variable values in `./partitions.py`
    ```python
    create_partitions_file( number_of_racks=4,
                            number_of_machines=12,
                            topic_name='test_topic',
                            number_of_partitions=16)
    ```

2. Run the following script:
    ```bash
    python partitions.py
    ```