from bloomfilter import BloomFilter 
from random import shuffle 

# words to be added 
word_present = [] 
count = 0
f = open("/usr/share/dict/american-english")
for x in f:
    if "'" in x:
        continue
    word_present.append(x)
    count += 1
n = count #no of items to add 
p = 0.5 #false positive probability 

bloomf = BloomFilter(n,p) 
print("Size of bit array:{}".format(bloomf.size)) 
print("False positive Probability:{}".format(bloomf.fp_prob)) 
print("Number of hash functions:{}".format(bloomf.hash_count)) 

# word not added 
word_absent = ['tanmay','rane','prashant'] 

for item in word_present: 
    bloomf.add(item) 

shuffle(word_present) 
shuffle(word_absent) 
test_words = ["apple","google"] + word_absent
shuffle(test_words) 
for word in test_words: 
	if bloomf.check(word): 
		if word in word_absent: 
			print("'{}' is a false positive!".format(word)) 
		else: 
			print("'{}' is probably present!".format(word)) 
	else: 
		print("'{}' is definitely not present!".format(word)) 
