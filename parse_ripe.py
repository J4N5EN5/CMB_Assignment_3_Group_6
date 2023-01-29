import os
import sys
import json
import bz2
import urllib.request

from math import sin, cos, sqrt, atan2, radians
from sty import fg, bg, ef, rs


def distance(x,y):
  R = 6373.0

  lat1 = radians(x[0])
  lon1 = radians(x[1])
  lat2 = radians(y[0])
  lon2 = radians(y[1])

  dlon = lon2 - lon1
  dlat = lat2 - lat1

  a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
  c = 2 * atan2(sqrt(a), sqrt(1 - a))

  distance = R * c

  return distance

def download(ripedir,filename):
  pprint("Downloading meta-latest as: "+filename)
  urllib.request.urlretrieve("https://ftp.ripe.net/ripe/atlas/probes/archive/meta-latest", ripedir+filename)

def unzip(ripedir,filename):
  pprint("Unziping as meta-latest.json")
  zipfile = bz2.BZ2File(ripedir+filename)
  data = zipfile.read()
  f = open("meta-latest.json", 'wb')
  f.write(data)
  f.close()
  pprint("Done!")

def pprint(s, level=0):
  if level == 0:
    print("["+fg.green+"INFO"+fg.rs+"] "+s)
  elif level == 1:
    print("["+fg.yellow+"WARN"+fg.rs+"] "+s)
  elif level == 2:
    print("["+fg.red+"ERROR"+fg.rs+"] "+s)


filename = ""
ripedir = "ripe/"

if not os.path.exists(ripedir):
  pprint("Please create "+ripedir+" folder first!", level=2)
  sys.exit(-1)


page = urllib.request.urlopen('https://ftp.ripe.net/ripe/atlas/probes/archive/')
for line in page.read().decode().split("\n"):
  if "meta-latest" in line:
    a = list(filter(lambda a: a != "", line.split(" ")))
    filename = a[2]+"-"+a[3]+".bz2"

exist = os.path.exists(ripedir+filename)
if not exist:
  pprint("New meta-latest exists!")
  download(ripedir,filename)
  unzip(ripedir,filename)
else:
  pprint(filename+" found!")
  pprint("Skipping download and unzip...")

if not os.path.exists("meta-latest.json"):
  if exist:
    unzip(ripedir,filename)
  else:
    pprint("Should not happen!",2)
    sys.exit(-1)

f = open("meta-latest.json")
data = json.loads(f.read())
f.close()
pprint("Reade File!")

starlink, home, lte = False, False, False
e_wlan = None
e_lte = None
for e in data["objects"]:
  if e["status_name"] == "Connected":
    
    if "lte" in e["tags"] and not lte:
      print("----------lte----------")
      print(e)
      print("----------lte----------")
      lte = True
      e_lte = e
    
    if "wlan" in e["tags"] and not home:
      print("----------wlan----------")
      print(e)
      print("----------wlan----------")
      home = True
      e_wlan = e

    if e["asn_v4"] == 14593 and not starlink:
      print("----------Starlink----------")
      print(e)
      print("----------Starlink----------")
      starlink = True
  
  if starlink and lte and home:
    break

distance = distance((e_lte["latitude"],e_lte["longitude"]),(e_wlan["latitude"],e_wlan["longitude"]))

print(f"Distance between lte and wlan: {distance}km")