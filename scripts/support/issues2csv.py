import csv
import json
import urllib.parse
import urllib.request


# See https://developer.github.com/v3/issues/


def get_issues(state=None, labels=None, milestones=None):
    query = dict()
    if state:
        query.update(state=state)
    if labels:
        query.update(labels=','.join(labels))
    if milestones:
        query.update(milestones=','.join(milestones))
    url = 'https://api.github.com/repos/CCI-Tools/cate-core/issues?' + urllib.parse.urlencode(query)
    with urllib.request.urlopen(url) as req:
        res = req.read()
        issues = json.loads(res.decode())
        issues = sorted(issues, key=lambda issue: issue['number'])
        return issues


# state = 'open'  # 'closed', 'all'
# labels = ['uc09', 'cli']
# milestones = ['v2.0']

milestone = 'v2.0'

issues = get_issues(milestones=[milestone])
issues = sorted(issues, key=lambda issue: issue['number'])

field_names = ['number', 'title', 'state', 'url']
with open('issues-%s.csv' % milestone, 'w', newline='') as fp:
    writer = csv.DictWriter(fp, field_names, delimiter=';')
    writer.writeheader()
    for issue in issues:
        writer.writerow({key: issue[key] for key in field_names})
