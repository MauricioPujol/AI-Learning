# Script que suma todos los dígitos encontrados en el .txt
import re
import os
sample_data_url = 'http://py4e-data.dr-chuck.net/regex_sum_42.txt'
actual_data_url = 'http://py4e-data.dr-chuck.net/regex_sum_1943028.txt'
if not os.path.exists('regex_sum_1943028.txt'):
    os.system(f'wget -c --read-timeout=5 --tries=0 "{actual_data_url}"')

file = open('regex_sum_1943028.txt','r')
numbers_sum = 0
for line in file:
    extracted_element = re.findall(r'\d+', line)
    if len(extracted_element) >0:
        numbers_sum += (sum(map(int,extracted_element)))

numbers_sum