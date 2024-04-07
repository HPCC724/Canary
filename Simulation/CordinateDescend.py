import csv
import sys
from functools import reduce

class ana_switch:
    def __init__(self,id):
        self.id = id
        self.linklist = []

class ana_link:
    def __init__(self,id,src,dst,gsp):
        self.id = id 
        self.src = src
        self.dst = dst
        self.ground_successprob = gsp
        if(self.ground_successprob != 1):
            self.faulty_flag = True
        self.sucessprob = 1
        self.pathlist = []


    def __str__(self):
        return self.src+'-'+self.dst

    def computeR(self,Lambda):
        square = 0
        for path in self.pathlist:
            square += path.computer(self.id)
        return square*2-2*Lambda
    
    def computeS(self,Lambda):
        mul = 0
        for path in self.pathlist:
            mul += path.computes(self.id)
        return 2*mul - Lambda

    def pred_correct(self):
        if(self.sucessprob == 1):
            if(self.ground_successprob == 1):
                return  -1
            else:
                return -2
        if(self.sucessprob != 1):
            if(self.ground_successprob != 1):
                return 1
            else:
                return 2

class ana_path:
    def __init__(self,id,strid,linklist,sp,samplesize = 100) :
        self.id = id
        self.strid = strid
        self.linklist = linklist
        self.successprob = sp
        self.samplesize = samplesize

    def __str__(self):
        string = ''
        string = string + str(self.linklist[0])
        for i in range(1,len(self.linklist)):
            string = string+'-'+self.linklist[i].dst
        return string
    
    def ximul(self):
        mul = 1
        for l in self.linklist:
            mul *= l.sucessprob
        return mul
    
    def computer(self,linkid):
        r= 1
        for tlink in self.linklist:
            if(tlink.id != linkid):
                r *= tlink.sucessprob
        return r**2

    def computes(self,linkid):
        r= 1
        for tlink in self.linklist:
            if(tlink.id != linkid):
                r *= tlink.sucessprob
        return r*self.successprob

def Coding_switch(src, dst):
    """编码2位link"""
    src = '0' + str(src) if src < 10 else str(src)
    dst = '0' + str(dst) if dst < 10 else str(dst)
    return src, dst

def data_read(bidir,pathfilename):
    switchfilename = "TopoSwitch.csv"
    linkfilename = "TopoLink.csv"
    switchidlen = 2
    linkidlen = 4
    switches = {}
    with open(switchfilename,"r") as f:
        reader = csv.reader(f)
        for row in reader:
            id = row[0]
            switchtemp = ana_switch(id)
            switches[id] = switchtemp
            
    links = {}
    with open(linkfilename,"r") as f:
        reader = csv.reader(f)
        for row in reader:
            id = row[0]
            prob = float(row[1])
            src = id[:switchidlen]
            dst = id[-switchidlen:]
            linktemp = ana_link(id,src,dst,prob)
            links[id] = linktemp

    pathes = {}
    with open(pathfilename,"r") as f:
        reader = csv.reader(f)
        for row in reader:
            id = row[0]
            strid = row[-1]
            prob = float(row[1])
            samplesize = int(row[2])
            linkids = row[-1]
            linklist = []
            pathlen = len(eval(row[-2]))-1
            for i in range (0,pathlen):
                startidx = int(i*switchidlen)
                endidx = int(startidx+linkidlen)
                linkid = linkids[startidx:endidx]
                linklist.append(links[linkid])
            pathtemp = ana_path(id,strid,linklist,prob)
            pathes[id] = pathtemp
            for link in pathes[id].linklist:
                link.pathlist.append(pathes[id])
    return switches,links,pathes

def Lcompute(paths,links,Lambda):
    lestmul = 0
    for p in paths.values():
        lestmul += p.successprob - p.ximul()
    lestmul = lestmul**2
    square = 0
    for l in links.values():
        square += l.sucessprob*(1-l.sucessprob)
    square *= Lambda
    return lestmul - square

def Coordinate_Descend(Epsilon,Lambda,iteration_num,pathes,links,switches):
    #init xi
    for k in links.keys():
        link = links[k]
        tempny = 0
        tempn = 0
        for p in link.pathlist:
            tempny += p.samplesize * p.successprob
            tempn += p.samplesize
        link.sucessprob = tempny*1.0/tempn
    loss = Lcompute(pathes,links,Lambda)
    print(loss)
    for i in range(0,iteration_num):
        for k in links.keys():
            link = links[k]
            Rk = link.computeR(Lambda)
            Sk = link.computeS(Lambda)
            if(Rk == 0):
                if Sk>0:
                    link.sucessprob = 1
                else:
                    link.sucessprob = 0  
            else:
                Tk = Sk/Rk
                if Rk > 0:
                    if Tk > 1:
                        link.sucessprob = 1
                    elif Tk < 0 :
                        link.sucessprob = 0
                    else:
                        link.sucessprob = Tk
                else:
                    if Tk > 0.5:
                        link.sucessprob = 0
                    else:
                        link.sucessprob = 1
        newloss = Lcompute(pathes,links,Lambda)
        print(newloss)
        if abs(newloss - loss) < Epsilon:
            break
        loss = newloss
    return pathes,links





if __name__ == '__main__':
    bidir = True  # True: paths and links are bi-directional
    Epsilon = 0.00000000001  # float: threshold of path error
    Lambda = 1  # tuning parameter of regularization  # 3:->1.6  5:bidir==True->1.8, bidir==False->1.7
    iteration_num = 100  
    pathfilename = "CANARY_path_segment_simulation.csv"
    # pathfilename = "NetBouncer_path_segment_simulation.csv"
    switches,links,pathes = data_read(False,pathfilename)
    print("read")
    Coordinate_Descend(Epsilon,Lambda,iteration_num,pathes,links,switches)
    # for l in links.values():
    #     print( str(l.sucessprob)+ ',' + str(l.ground_successprob))
    
    outputfilename = 'predout.csv'
    with open(outputfilename,'w',newline='') as of:
        csvwriter = csv.writer(of)
        for l in links.values():
            temp = []
            temp.append(l.src+'-'+l.dst)
            temp.append(l.sucessprob)
            temp.append(l.ground_successprob)
            csvwriter.writerow(temp)

    faillinkfilename = 'detected_failinks.csv'
    with open(faillinkfilename,'w',newline='') as of:
        csvwriter = csv.writer(of)
        for l in links.values():
            if(l.sucessprob != 1 and l.ground_successprob != 1):
                temp = []
                temp.append(l.src+'-'+l.dst)
                temp.append(l.sucessprob)
                temp.append(l.ground_successprob)
                csvwriter.writerow(temp)
    intervals = [[0.2,1],[0.1,0.2],[0.05,0.1],[0.01,0.05],[0.001,0.01]]



    anasisoutfilename = 'ansysout.csv'
    segment_anasisoutfilename = 'anasysout_segment.csv'
    segment_num = len(intervals)
    tp_seg = [0]*segment_num
    fp_seg = [0]*segment_num
    tn_seg = [0]*segment_num
    fn_seg = [0]*segment_num


    tp = 0
    fp = 0
    tn = 0
    fn = 0
    for l in links.values():
        correctn = l.pred_correct()
        if correctn == -1:
            tn += 1
            for i in range(0,len(intervals)):
                if (1-l.ground_successprob) > intervals[i][0] and 1-l.ground_successprob <= intervals[i][1]:
                    tn_seg[i]+=1
                    break
        elif correctn == -2:
            fn += 1
            for i in range(0,len(intervals)):
                if (1-l.ground_successprob) > intervals[i][0] and 1-l.ground_successprob <= intervals[i][1]:
                    fn_seg[i]+=1
                    break
        elif correctn == 1:
            tp += 1
            for i in range(0,len(intervals)):
                if (1-l.ground_successprob) > intervals[i][0] and 1-l.ground_successprob <= intervals[i][1]:
                    tp_seg[i]+=1
                    break
        else:
            fp += 1
            for i in range(0,len(intervals)):
                if (1-l.ground_successprob) > intervals[i][0] and 1-l.ground_successprob <= intervals[i][1]:
                    fp_seg[i]+=1
                    break

    faultpathes = []
    for p in pathes.values():
        if p.successprob != 1:
            faultpathes.append(p)
    with open("FaultyPath.csv",'w',newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        for p in faultpathes:
            csvwriter.writerow([str(p),p.successprob])

    with open(anasisoutfilename,'w',newline='') as of:
        csvwriter = csv.writer(of)
        temp = ['tp','fp','tn','fn','total']
        csvwriter.writerow(temp)
        temp = [tp,fp,tn,fn,len(links)]
        csvwriter.writerow(temp)

    with open(segment_anasisoutfilename,'w',newline='') as of:
        csvwriter = csv.writer(of)
        temp = ['segment','tp','fp','tn','fn','total']
        csvwriter.writerow(temp)
        for i in range(0,len(intervals)):
            temp = [intervals[i],tp_seg[i],fp_seg[i],tn_seg[i],fn_seg[i]]
            csvwriter.writerow(temp)
        temp = [['-MAX_DOUBLE,0.001'],0,fp,0,0]
        csvwriter.writerow(temp)