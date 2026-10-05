"""Offline validation helpers. Metrics are calculated only from authentic observations."""
import json,math
from pathlib import Path

def regression_metrics(y,p):
    if not y: return {}
    n=len(y); mae=sum(abs(a-b) for a,b in zip(y,p))/n; rmse=math.sqrt(sum((a-b)**2 for a,b in zip(y,p))/n)
    mean=sum(y)/n; ss=sum((a-mean)**2 for a in y); r2=1-sum((a-b)**2 for a,b in zip(y,p))/ss if ss else None
    return {"mae_m":mae,"rmse_m":rmse,"r2":r2,"n":n}

def classification_metrics(actual,pred,danger):
    a=[x>=danger for x in actual]; p=[x>=danger for x in pred]
    tp=sum(x and y for x,y in zip(a,p)); tn=sum((not x) and (not y) for x,y in zip(a,p)); fp=sum((not x) and y for x,y in zip(a,p)); fn=sum(x and (not y) for x,y in zip(a,p))
    return {"accuracy":(tp+tn)/len(a) if a else None,"precision":tp/(tp+fp) if tp+fp else None,"recall":tp/(tp+fn) if tp+fn else None,"f1":2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None,"false_alarm_rate":fp/(fp+tn) if fp+tn else None,"missed_flood_rate":fn/(fn+tp) if fn+tp else None,"tp":tp,"tn":tn,"fp":fp,"fn":fn}
