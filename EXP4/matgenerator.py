import random

f= open("/media/tanmay/Data/SEM-8/BDA/EXP4/post_input_1","a")

MAX = 100; 
for i in range(MAX):
    for j in range(MAX):
        f.write("A,"+str(i)+","+str(j)+","+str(random.choice([1,4,8,10,3]))+"\n")
f.close()
