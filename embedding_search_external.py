#!/usr/bin/env python3
import sys, csv, os, argparse
from sentence_transformers import SentenceTransformer
import numpy as np

def load_combo_unmapped(path='combo_umls_search_results.tsv'):
    rows=[]
    with open(path) as f:
        for line in f:
            parts=line.rstrip('\n').split('\t')
            if len(parts)<3: parts += ['']*(3-len(parts))
            if parts[2].strip()=='':
                rows.append((parts[0], parts[1].replace('_',' ')))
    return rows

def load_semmed(path):
    # expect CSV with name in column 1 or header containing 'name' or 'concept'
    names=[]
    with open(path, newline='') as f:
        reader=csv.reader(f)
        for r in reader:
            if not r: continue
            # pick longest cell as candidate name
            name=max(r, key=lambda x: len(x))
            names.append(name)
    return names

def load_ikraph_flat(path):
    # plain TSV with id\tname or similar
    names=[]
    with open(path) as f:
        for line in f:
            parts=line.rstrip('\n').split('\t')
            if len(parts)>=2:
                names.append(parts[1])
            else:
                names.append(parts[0])
    return names


def embed_and_search(combo_unmapped, external_names, model_name='all-MiniLM-L6-v2', topk=5):
    model=SentenceTransformer(model_name)
    combo_texts=[c[1] for c in combo_unmapped]
    print(f'Embedding {len(combo_texts)} combo labels and {len(external_names)} external names',file=sys.stderr)
    combo_emb=model.encode(combo_texts, batch_size=64, show_progress_bar=False)
    ext_emb=model.encode(external_names, batch_size=64, show_progress_bar=False)
    # normalize
    def norm(x):
        return x / (np.linalg.norm(x,axis=1,keepdims=True)+1e-9)
    combo_n=norm(combo_emb)
    ext_n=norm(ext_emb)
    sims = np.dot(combo_n, ext_n.T)
    # produce results
    out_rows=[]
    for i,(cid,clabel) in enumerate(combo_unmapped):
        row = sims[i]
        idxs = np.argsort(row)[-topk:][::-1]
        for rank,k in enumerate(idxs,1):
            out_rows.append((cid,clabel,rank,k,external_names[k],float(row[k])))
    return out_rows


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--semmed', help='SemMed concept CSV path')
    p.add_argument('--ikraph', help='iKraph flat names TSV (id\tname)')
    p.add_argument('--out', default='combo_external_embedding_matches.tsv')
    p.add_argument('--topk', type=int, default=5)
    args=p.parse_args()

    combo_unmapped = load_combo_unmapped()
    if not combo_unmapped:
        print('No unmapped COMBO labels found', file=sys.stderr)
        sys.exit(1)

    external_names=[]
    source_ids=[]
    if args.semmed and os.path.exists(args.semmed):
        semmed_names = load_semmed(args.semmed)
        external_names += [f'SEMMED: {n}' for n in semmed_names]
        source_ids += ['SEMMED']*len(semmed_names)
    if args.ikraph and os.path.exists(args.ikraph):
        ik_names = load_ikraph_flat(args.ikraph)
        external_names += [f'IKRAPH: {n}' for n in ik_names]
        source_ids += ['IKRAPH']*len(ik_names)

    if not external_names:
        print('No external names provided or files not found', file=sys.stderr)
        sys.exit(1)

    # strip source prefixes before embedding so embedding model sees the raw text
    clean_names = [n.split(': ',1)[1] if ': ' in n else n for n in external_names]
    results = embed_and_search(combo_unmapped, clean_names, topk=args.topk)

    # write out
    with open(args.out,'w',newline='') as fo:
        w=csv.writer(fo, delimiter='\t')
        w.writerow(['combini_id','combini_label','rank','ext_index','ext_name','score'])
        for r in results:
            w.writerow([r[0], r[1], r[2], r[3], r[4], f'{r[5]:.4f}'])
    print('Wrote', args.out, file=sys.stderr)

if __name__=='__main__':
    main()
