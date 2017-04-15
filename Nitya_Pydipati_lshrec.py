from pyspark import SparkContext
from collections import defaultdict
import sys
import itertools



def computeminhash(user,i):
        
    minhash= (3*(user[0])+(13*i))%100
    
    return [(users,minhash) for users in user[1]]

def candidates(items):
    candidate=zip(items[0],items[1],items[2],items[3])
    pairs=defaultdict(list)
    for i in xrange(0,len(candidate)):
        pairs[candidate[i]].append(i)
    return pairs

def jaccard(pair,out):
    out=dict(out)
    a= set(out[int(pair[0])+1])  
    b= set(out[int(pair[1])+1])
    common = len(a.intersection(b))
    uni=len(a.union(b))
    similarity_jac=float(common)/float(uni)
    return ((pair[0],(pair[1],similarity_jac)),(pair[1],(pair[0],similarity_jac)))
    
def similar(x,y):
    compar_sim=cmp(y[1],x[1])
    compar_users=cmp(x[0],y[0])
    if compar_sim==0:
        return compar_users
    else:
        return compar_sim
    
def compare(sim):
    sim.sort(similar)
    if(len(sim)>=5):
        length=5
    else:
        length=len(sim)
    return sim[:length]


def main():
    sc=SparkContext()
    
    users_movies_rdd=sc.textFile(sys.argv[1])
    outsput=sys.argv[2]
    user=users_movies_rdd.map(lambda x: (x.split(',')[0],x.split(',')[1:])).flatMapValues(lambda x:x).map(lambda(x,y):(int(str(x).split('U')[1]),int(y)))
    out=sc.broadcast(user.groupByKey().collect()).value


    user=user.map(lambda (x,y):(y,x))
    signatures=[user.groupByKey().flatMap(lambda x: computeminhash(x,i)).reduceByKey(lambda x,y:x if x<y else y).sortByKey().map(lambda (x,y): y).collect() for i in xrange(0,20)]
    
    
    bands=sc.parallelize(signatures,5)

    candidate_pairs=bands.mapPartitions(lambda x: [candidates(list(x))])

    pairs=candidate_pairs.flatMap(lambda x:x.values()).map(lambda x: x if len(x)>1 else None).filter(lambda x: x!=None)
    comb_pairs=pairs.flatMap(lambda x: [list(candidates) for candidates in itertools.combinations(x, 2)])

    comb_pairs=comb_pairs.groupBy(lambda (x,y): ((x,y))).map(lambda (x,y):x) 
    comb_pair=sc.parallelize(comb_pairs.collect(),500)

    similarity=comb_pair.flatMap(lambda x: jaccard(x,out)).groupByKey()

    top_user=similarity.map(lambda (x,y): (x,compare(list(y)))).map(lambda (x,y):(x+1,y)).flatMap(lambda (x,y): [(x,y1[0]+1) for y1 in sorted(y)])
    final_top_user=top_user.map(lambda (x,y): (x,y)).groupByKey()
    out_final=final_top_user.collect()
    out_final=sorted(out_final)
    f=open(outsput, 'w')
    finall=[]
    for o,u in out_final:
        la=""
        la= "U"+str(o)+":"
        la1=""
        la1=",".join(["U"+str(u1) for u1 in sorted(list(set(list(u))))]) 
        la2=la+la1+"\n"
        finall.append(la2)
    for f1 in finall:
        f.write(f1)   
if __name__=="__main__":
    main()

