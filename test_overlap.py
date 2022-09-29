import pandas as pd
pair_list=[]


def get_indexes(x):
    n=0
    
    for n in range(len(x)):
        for b in range(len(x)):
            

            pair=[n,b]
            if pair not in pair_list and n!=b and n<b:
                pair_list.append(pair)
                # print(pair_list)
    return pair_list


def overlap_intersect_check(x):
    v=False
    off_list=[]
    indexes=get_indexes(x)
    # print(indexes)
    for i1,i2 in indexes:
        # print(x[i1],x[i2])
        
        s=x[i1]
        s2=x[i2]
        bmp1,emp1=s[0],s[1]
        bmp2,emp2=s2[0],s2[1]
        if _overlap(bmp1,emp1,bmp2,emp2) == True:
            v=True
            off_list.append(i1)
            off_list.append(i2)
    return off_list,v
               
def _overlap(bmp1,emp1,bmp2,emp2,debug=False):
    bmp_overlap1 = bmp2 < bmp1 and emp2 > bmp1
    emp_overlap1 = bmp2 < emp1 and emp2 > emp1
    bmp_overlap2 = bmp1 < bmp2 and emp1 > bmp2
    emp_overlap2 = bmp1 < emp2 and emp1 > emp2

    if debug: print(bmp_overlap1,emp_overlap1,bmp_overlap2,emp_overlap2,[bmp1,emp1,bmp2,emp2])
    return (bmp_overlap1 or 
     emp_overlap1 or 
     bmp_overlap2 or 
     emp_overlap2)



df=pd.read_csv(r'C:\PythonTest\Voltron\district_chrystal_report_website\hpms-validation\hpms_data_items\DataItem10_Peak_Lanes.csv',sep='|')
meh= [df['BeginPoint'],df['EndPoint']]
# print(meh)
test=overlap_intersect_check(meh)
print(test)