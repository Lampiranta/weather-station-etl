# weather-station-etl
Take climate measurements sent by esp32 to azure blob storage, transform the data to suitable format and send it to next step.



Content in body:
- name* : string
- bda* : string
- temperature* : double
- humidity* : double
- pressure* : int
- accelerometerX* : double
- accelerometerY* : double
- accelerometerZ* : double
- battery* : int
- txpower* : int
- moves* : int
- sequence* : int

Where * is index of result, body may contain 1 or more instances of data. 

Example:
```json
{'name1': 'tyohuone', 'bda1': 'D4:87:59:1C:C8:1', 'temperature1': 21.47, 'humidity1': 36.77, 'pressure1': 99972, 'accelerometerX1': -0.99, 'accelerometerY1': -0.21, 'accelerometerZ1': 0.08, 'battery1': 3, 'txpower1': 4, 'moves1': 199, 'sequence1': 49041, 'name2': 'makuuhuone', 'bda2': 'E3:B3:0F:3B:F8:C', 'temperature2': 21.79, 'humidity2': 36.32, 'pressure2': 100043, 'accelerometerX2': 1.01, 'accelerometerY2': 0.11, 'accelerometerZ2': 0.01, 'battery2': 2.99, 'txpower2': 4, 'moves2': 105, 'sequence2': 61087}
```