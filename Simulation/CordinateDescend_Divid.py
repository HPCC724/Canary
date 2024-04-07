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
        self.faillink = False   #if self.faillink == true , its sucessprob is always 0

    def __str__(self):
        return self.src+'-'+self.dst

    def LinkInInterval(self,interval_left,interval_right):
        if self.ground_successprob <= interval_right and self.ground_successprob > interval_left:
            return True
        else:
            return False

    def computeR_interval(self,Lambda,interval_left,interval_right):
        square = 0
        for path in self.pathlist:
            square += path.computer_interval(self.id,interval_left,interval_right)
        return square * 2 - 2 * Lambda

    def computeS_interval(self,Lambda,interval_left,interval_right):
        mul = 0
        for path in self.pathlist:
            mul += path.computes_interval(self.id,interval_left,interval_right)
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
    
    def GetLinkSp(self):
        if self.faillink == False:
            return self.sucessprob
        else:
            return 0

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

    def InInterval(self,interval_left,interval_right):
        if self.successprob <= interval_right and self.successprob > interval_left:
            return True
        else:
            return False

    def FaultLinksinPath(self):
        ftlinksinpath = []
        for l in self.linklist:
            if l.ground_successprob != 1:
                ftlinksinpath.append(l)
        return ftlinksinpath

    def GetPathSp(self,interval_left,interval_right):
        #high sp, 1
        if self.successprob > interval_right:
            return 1
        #sp in between, projection
        elif self.successprob > interval_left:
            return self.SpProjection(interval_left,interval_right,self.successprob)
        else:
            return self.successprob

    def SpProjection(self,interval_left,interval_right,sp_input):
        target_left = 0
        target_right = 0.8
        sp_output = (sp_input-interval_left)/(interval_right-interval_left)*(target_right-target_left)
        return sp_output

    def ximul(self):
        mul = 1
        for l in self.linklist:
            mul *= l.GetLinkSp()
        return mul
    
    def computer_interval(self,linkid,interval_left,interval_right):
        r= 1
        for tlink in self.linklist:
            if(tlink.id != linkid):
                r *= tlink.GetLinkSp()
        return r**2

    def computes_interval(self,linkid,interval_left,interval_right):
        s= 1
        for tlink in self.linklist:
            if(tlink.id != linkid):
                s *= tlink.GetLinkSp()
        return s * self.GetPathSp(interval_left, interval_right)


def Coding_switch(src, dst):
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

def Lcompute_interval(paths,links,Lambda,interval_left,interval_right):
    lestmul = 0
    for p in paths.values():
        lestmul += p.GetPathSp(interval_left,interval_right) - p.ximul()
    lestmul = lestmul**2
    square = 0
    for l in links.values():
        square += l.GetLinkSp()*(1-l.GetLinkSp())
    square *= Lambda
    return lestmul - square

def Coordinate_Descend_Interval(Epsilon,Lambda,iteration_num,pathes,links,switches,interval_left,interval_right):
    #init xi
    for k in links.keys():
        link = links[k]
        if link.faillink == True:
            continue
        tempny = 0
        tempn = 0
        for p in link.pathlist:
            tempny += p.samplesize * p.GetPathSp(interval_left,interval_right)
            tempn += p.samplesize
        link.sucessprob = tempny*1.0/tempn
    loss = Lcompute_interval(pathes,links,Lambda,interval_left,interval_right)
    print(loss)
    for i in range(0,iteration_num):
        for k in links.keys():
            link = links[k]
            if(link.faillink == True):
                continue
            Rk = link.computeR_interval(Lambda,interval_left,interval_right)
            Sk = link.computeS_interval(Lambda,interval_left,interval_right)
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
        newloss = Lcompute_interval(pathes,links,Lambda,interval_left,interval_right)
        print(newloss)
        if abs(newloss - loss) < Epsilon:
            break
        loss = newloss

    for l in links.values():
        if l.faillink == False and l.sucessprob != 1:
            l.faillink = True
    return pathes,links

def readinterval():
    pass


if __name__ == '__main__':
    bidir = True  # True: paths and links are bi-directional
    Epsilon = 0.00000000001  # float: threshold of path error
    Lambda = 1  # tuning parameter of regularization  # 3:->1.6  5:bidir==True->1.8, bidir==False->1.7
    iteration_num = 100  
    pathfilename = "CANARY_path_segment_simulation.csv"
    # pathfilename = "NetBouncer_path_segment_simulation.csv"
    # pathfilename = "CANARY_path_segment.csv"
    switches,links,pathes = data_read(False,pathfilename)
    print("read")
    #interval file
    intervelfilename = ""
    intervals = [[0.2,1],[0.1,0.2],[0.05,0.1],[0.01,0.05],[0.001,0.01]]

    faultlinks = []
    faultlinks_interval = [[],[],[],[],[]]


    faultpathes = []
    for p in pathes.values():
        if p.successprob != 1:
            faultpathes.append(p)
    with open("FaultyPath.csv",'w',newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        for p in faultpathes:
            csvwriter.writerow([str(p),p.successprob])


    faultpathes_interval = [[],[],[],[],[]]
    gdfaillinkinPathes_interval = [[],[],[],[],[]]

    falsePositivelinks_interval = [[],[],[],[],[]]
    falseNegativelinks_interval =  [[],[],[],[],[]]

    for i in range(0,len(intervals)):
        inter = intervals[i]
        Coordinate_Descend_Interval(Epsilon,Lambda,iteration_num,pathes,links,switches,1-inter[1],1-inter[0])


        for l in links.values():
            if l. LinkInInterval(1-inter[1],1-inter[0]):
                gdfaillinkinPathes_interval[i].append(l)
            if(l.faillink == True and l not in faultlinks):
                faultlinks.append(l)
                faultlinks_interval[i].append(l)
                if(l.ground_successprob == 1):
                    falsePositivelinks_interval[i].append(l)

        for gdfli in gdfaillinkinPathes_interval[i]:
            if gdfli not in faultlinks_interval[i] and gdfli not in faultlinks:
                falseNegativelinks_interval[i].append(gdfli)

    NotDetectedPathes = []
    NotDetectedLinks = []
    for p in pathes.values():
        if p.successprob == 1 and len(p.FaultLinksinPath()) !=0:
            NotDetectedPathes.append(p)

    for l in links.values():
        if l.ground_successprob!= 1 and l not in faultlinks:
            NotDetectedLinks.append(l)

    segmentdetectedfilename = 'segment_detected.csv'
    with open(segmentdetectedfilename,'w',newline='') as of:
        csvwriter = csv.writer(of)
        for i in range(0,len(intervals)):
            csvwriter.writerow(intervals[i])
            gdlinktemp = ['ground-truth bad links']
            gdlinktemp.append(len(gdfaillinkinPathes_interval[i]))
            for l in gdfaillinkinPathes_interval[i]:
                gdlinktemp.append(l)
            csvwriter.writerow(gdlinktemp)

            dtfailinktemp = ['detected bad links']
            dtfailinktemp.append(len(faultlinks_interval[i]))
            for l in faultlinks_interval[i]:
                dtfailinktemp.append(l)
            csvwriter.writerow(dtfailinktemp)

            fptemp = ['false_positive']
            fptemp.append(len(falsePositivelinks_interval[i]))
            for l in falsePositivelinks_interval[i]:
                fptemp.append(l)
            csvwriter.writerow(fptemp)

            fntemp = ['false_negative']
            fntemp.append(len(falseNegativelinks_interval[i]))
            for l in falseNegativelinks_interval[i]:
                fntemp.append(l)
            csvwriter.writerow(fntemp)

            csvwriter.writerow([])

        csvwriter.writerow(['notdetected_links'])
        csvwriter.writerow(NotDetectedLinks)

    # for l in links.values():
    #     print( str(l.sucessprob)+ ',' + str(l.ground_successprob))
    
    outputfilename = 'predout.csv'
    with open(outputfilename,'w',newline='') as of:
        csvwriter = csv.writer(of)
        for l in links.values():
            temp = []
            temp.append(l.src)
            temp.append(l.dst)
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
        temp = [['0,0.001'],0,fp,0,0]
        csvwriter.writerow(temp)
