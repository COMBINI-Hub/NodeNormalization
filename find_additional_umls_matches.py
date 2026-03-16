"""
Search UMLS for COMBINI concepts that still lack a CUI and auto-patch high-confidence matches.

Criteria for auto-patch:
 - recall score >= 0.90 (fraction of query words present in candidate)
 - OR exact normalized-string equality

Usage:
  UMLS_API_KEY=... python3 find_additional_umls_matches.py

Outputs:
 - updates `combo_umls_search_results.tsv` (creates .bak)
 - writes `additional_umls_candidates.tsv` with candidate matches and scores
"""
import csv
import html
import os
import re
import requests
from pathlib import Path

UMLS_TSV = Path("/Users/drshika2/NodeNormalization/combo_umls_search_results.tsv")
BACKUP = UMLS_TSV.with_suffix('.tsv.bak')
OUTPUT_CAND = Path("/Users/drshika2/NodeNormalization/additional_umls_candidates.tsv")
API_KEY = os.environ.get('UMLS_API_KEY')
if not API_KEY:
    API_KEY = input('Enter UMLS API key: ').strip()

SEARCH_URL = 'https://uts-ws.nlm.nih.gov/rest/search/current'

_re_sep   = re.compile(r"[_\-/]")
_re_strip = re.compile(r"[^a-z0-9 ]")
_re_ws    = re.compile(r"\s+")

def normalize(s: str) -> str:
    s = html.unescape(s or '')
    s = s.replace('_', ' ')
    s = s.lower()
    s = _re_sep.sub(' ', s)
    s = _re_strip.sub('', s)
    return _re_ws.sub(' ', s).strip()

def recall_score(query_words: set, candidate: str) -> float:
    c = set(normalize(candidate).split())
    if not query_words:
        return 0.0
    return len(query_words & c) / len(query_words)

print('Loading TSV...')
with open(UMLS_TSV, newline='\n', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    rows = list(reader)
    header = reader.fieldnames

unmatched = [r for r in rows if not r.get('umls_cui')]
print(f'Found {len(unmatched)} unmatched rows to check')

candidates = []
patched = 0

for i, r in enumerate(unmatched, 1):
    label = html.unescape(r.get('combini_label','')).replace('_',' ')
    q = normalize(label)
    q_words = set(q.split())
    best = None
    best_score = 0.0
    for stype in ('normalizedString','words'):
        try:
            resp = requests.get(SEARCH_URL, params={
                'string': label,
                'apiKey': API_KEY,
                'searchType': stype,
                'returnIdType': 'concept',
                'pageSize': 50,
            }, timeout=15)
            data = resp.json()
            results = data.get('result',{}).get('results', [])
        except Exception as e:
            results = []
        for res in results:
            name = res.get('name','')
            cui = res.get('ui','')
            src = res.get('rootSource','')
            sc = recall_score(q_words, name)
            if normalize(name) == q:
                sc = 1.0
            if sc > best_score:
                best_score = sc
                best = (cui, name, src, stype, sc)
    if best:
        candidates.append({
            'combini_id': r.get('combini_id',''),
            'combini_label': r.get('combini_label',''),
            'candidate_cui': best[0],
            'candidate_name': best[1],
            'candidate_src': best[2],
            'search_type': best[3],
            'score': f'{best[4]:.3f}',
        })
        # Auto-patch rules
        if best[4] >= 0.90 or best[4] == 1.0:
            # find row in rows and patch
            for rr in rows:
                if rr.get('combini_label') == r.get('combini_label'):
                    rr['umls_cui'] = best[0]
                    rr['umls_name'] = best[1]
                    rr['umls_rootSource'] = best[2]
                    rr['similarity_score'] = f'{best[4]:.3f}'
                    rr['all_matching_cuis'] = f"{best[0]}:{best[1]}:{best[2]}"
                    rr['search_type'] = best[3]
                    rr['query_used'] = label
                    patched += 1
                    break

print(f'Found {len(candidates)} candidate matches, auto-patched {patched} rows')

with open(OUTPUT_CAND, 'w', newline='\n', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['combini_id','combini_label','candidate_cui','candidate_name','candidate_src','search_type','score'], delimiter='\t')
    w.writeheader()
    for c in candidates:
        w.writerow(c)

if patched:
    print('Backing up original TSV to', BACKUP)
    BACKUP.write_text(UMLS_TSV.read_text(encoding='utf-8'), encoding='utf-8')
    print('Writing patched TSV...')
    with open(UMLS_TSV, 'w', newline='\n', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=header, delimiter='\t')
        w.writeheader()
        for rr in rows:
            w.writerow(rr)
    print('Patched TSV written.')
else:
    print('No rows auto-patched; candidates written to', OUTPUT_CAND)
