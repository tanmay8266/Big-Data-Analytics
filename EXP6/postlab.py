from bloomfilter import BloomFilter 
from random import shuffle 

# words to be added 
word_present = [] 
count = 0
f = open("/media/tanmay/Data/SEM-8/BDA/EXP6/passwords")
for x in f:
    if "'" in x:
        continue
    word_present.append(x)
    count += 1
n = count #no of items to add 
p = 0.3 #false positive probability 

bloomf = BloomFilter(n,p) 
print("Size of bit array:{}".format(bloomf.size)) 
print("False positive Probability:{}".format(bloomf.fp_prob)) 
print("Number of hash functions:{}".format(bloomf.hash_count))  

for item in word_present: 
    bloomf.add(item) 

shuffle(word_present)
while(True):
    passwd = input("Please Enter a Password")
    if bloomf.check(passwd):
        print("'{}' is probably present please enter a different password!".format(passwd)) 
    else: 
        print("'{}' is definitely not present it's added successfully!".format(passwd))
        break 
