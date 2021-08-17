"""
https://github.com/KBNLwikimedia/SDoC/tree/main/writeSDoCfromExcel

This scripts writes Property-Qid pairs from an Excel sheet to the Structured Data of a file on Wikimedia Commons using the Commons API.
For instance it can write P180-Depicts --> Q284865 to https://commons.wikimedia.org/wiki/File:Atlas_Schoemaker-UTRECHT-DEEL1-3120-Utrecht,_Utrecht.jpeg
See P180Inputfile.xlsx for the expected input format and column names (This example Excel is about adding P180-Depicts values to a Commons file,
but this script is also able to add Wikidata Qids to other properties (than P180) of the Structured Data of a file

Script written by User:OlafJanssen, many thanks to User:Multichill
Feel free to adapt, license = CC0
"""
import requests
import json
import os, os.path
import pandas as pd
from pywikibot.comms import http

###############################################################
def addClaim(mediaid, pid, qid, summary):
    # Taken & modified from https://github.com/multichill/toollabs/blob/1d5ef0ea24333a4918d388fe0fdade12d97b66ac/bot/erfgoed/wikidata_to_monuments_list.py
    """
    Add a claim to a mediaid
    :param mediaid: The mediaid to add it to
    :param pid: The property P id (including the P)
    :param qid: The item Q id (including the Q)
    :param summary: The summary to add in the edit
    :return: Nothing, edit in place
    """
    # print(cookies)
    # We have now logged in and can request edit tokens thusly:
    tokenrequest = requests.post(api_url, data={'action': 'query',
                                                'format': 'json',
                                                'meta': 'tokens'},
                                          cookies=cookies)
    # The cookies=cookies at the end is essential for an upload by a logged-in user,
    # If it is omitted, the IP-addres of the uploader will be displayed in the file History,
    # as if the user were not logged in
    tokendata = json.loads(tokenrequest.text)
    token = tokendata.get(u'query', 'XX').get(u'tokens', 'XX').get(u'csrftoken', 'XX')
    postvalue = {"entity-type": "item", "numeric-id": qid.replace(u'Q', u'')}
    postdata = {u'action': u'wbcreateclaim',
                u'format': u'json',
                u'entity': mediaid,
                u'property': pid,
                u'snaktype': u'value',
                u'value': json.dumps(postvalue),
                u'token': token,
                u'summary': summary,
                u'bot': False,
                }
    wbcreateclaim_response = http.fetch(u'https://commons.wikimedia.org/w/api.php', method='POST', data=postdata, cookies=cookies)
    wbcreateclaimdata = wbcreateclaim_response.json()
    print('Response from Commons API: %s' % (wbcreateclaimdata))

def getPropertyQids(mediaid, property):
    # Taken & modified from https://github.com/multichill/toollabs/blob/1d5ef0ea24333a4918d388fe0fdade12d97b66ac/bot/erfgoed/wikidata_to_monuments_list.py
    # Code partly from https://kbnlwikimedia.github.io/KBCollectionHighlights/stories/Cool%20new%20things%20you%20can%20now%20do%20with%20the%20KB's%20collection%20highlights/Part%205%2C%20Reuse.html
    # Item 47 on that page
    """
    :param mediaid: The entity ID (like M1234, pageid prefixed with M)
    :param property: The property ID to check for (like P180)
    :return: qlist [] of existing Qids for the given Property (like P180)
    """
    apiurl = 'https://commons.wikimedia.org/w/api.php?action=wbgetentities&ids=%s&&format=json' % (mediaid)
    headers = {'Accept': 'application/json', 'User-Agent': 'User OlafJanssen - %s - %s' % (mediaid, property)}
    response = requests.get(apiurl, headers=headers)
    data = json.loads(response.text)
    props = data.get('entities', 'XX').get(mediaid).get('statements', 'XX').get(property, 'XX')
    #print('Props: %s' % (props))
    qlist = []
    if str(props) != 'XX':
       for p in range(0, len(props)):
           qid= props[p].get('mainsnak', 'XX').get('datavalue', 'XX').get('value').get('id', 'XX')
           qlist.append(qid)
    else: print('In file %s there are no Qids for %s yet' % (mediaid,property))
    return qlist

############################################################################
api_url = 'https://commons.wikimedia.org/w/api.php'

# Your Wikimedia credentials
# If left blank, or if incorrect (eg wrong passwd), the edit will still be done, but will be shown as done from your IP address
USER=u'your-wikimedia-username'
PASS=u'your-wikimedia-passwd'
USER_AGENT='%s adding Qids to SDoC using a Python script and the Common API' % (USER)

headers={'User-Agent': USER_AGENT}
# get login token and log in
payload = {'action': 'query', 'format': 'json', 'utf8': '', 'meta': 'tokens', 'type': 'login'}
r1 = requests.post(api_url, data=payload)
login_token=r1.json().get(u'query', 'XX').get(u'tokens', 'XX').get(u'logintoken', 'XX')
login_payload = {'action': 'login', 'format': 'json', 'utf8': '','lgname': USER, 'lgpassword': PASS, 'lgtoken': login_token}
r2 = requests.post(api_url, data=login_payload, cookies=r1.cookies)
cookies = r2.cookies.copy()

########################################################################
currentdir = os.path.dirname(os.path.realpath(__file__))  # Path of this .py file
filename = "P180Inputfile.xlsx" #name of Excel file, assumed to be in the same dir as this script
excelpath = currentdir + "\\" + filename
sheetname = "CommonsMnumbersWikidataQnumbers"
# Tip: Use Minefield tool (https://hay.toolforge.org/minefield/) and OpenRefine (or even https://github.com/hay/wdreconcile)  to fill the sheet
df = pd.read_excel(excelpath, sheet_name=sheetname, header=0)
df.fillna(0, inplace=True) #fill empty cells with 0
df2=df[['CommonsMid','QidDepicts','CommonsFile']]
dfdict=df2.to_dict(orient='records')

# Target property to add Qids to
property = "P180"

#for i in range(0,3):
#for i in range(100,200):
for i in range(0,len(df2)):
    rowdict = dfdict[i]
    qid = rowdict.get('QidDepicts', 'XX')
    if str(qid) != 'XX':
        commonsmid = rowdict.get('CommonsMid', 'XX')
        commonsfile = rowdict.get('CommonsFile', 'XX')
        print("Trying to add %s to %s in Commons file %s (= %s)" % (qid, property, commonsmid,  commonsfile))
        plist = getPropertyQids(commonsmid,property)
        print("Already existing Qids for %s in Commons file %s: %s" % (property, commonsmid, plist))
        if qid not in plist:
            editsummary= 'Added %s to %s in %s (= %s) via the Commons API' % (qid, property, commonsmid, commonsfile)
            print('Editsummary: %s' % (editsummary))
            # Now do the actual write/post to the API
            addClaim(commonsmid, property, qid, editsummary)
            print('SUCCESS: %s successfully added to %s in %s (= %s)' % (qid, property, commonsmid, commonsfile))
        else: print("SKIPPED: Did not add %s to %s in Commons file %s, as it already exists" % (qid, property, commonsmid))
    else: print('ERROR -- Something went wrong: str(qid) = "XX"')
    print('-'*40)

