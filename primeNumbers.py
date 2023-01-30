import os
def function(x,y):
    list1=[]
    for i in range(x,y+1):
        count_div=0
        #if ( i == 1):
         # count_div =1
        for j in range (1,i):
          #print ("i and j",  i,j)
          total_count=divisor(i,j)
          count_div +=total_count
        #print('total Divisors for i Resp. : ',i,count_div )
        if (count_div < 2 and i != 0):
          list1.append(i)
          os.system('cls')
    print(list1)
          

def divisor(input1,input2):
  if (input1%input2 == 0 or input1 == 1):
    return 1
  else:
    return 0

			
function(int(1),int(50))
