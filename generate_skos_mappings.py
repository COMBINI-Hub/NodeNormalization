"""
Generate SKOS TTL mappings from `combo_umls_search_results.tsv`, `combo_umls_atoms.tsv`, and `combo_ikraph_semmed_results.tsv`.
Creates `COMBO_skos_mappings.ttl` with triples using `skos:exactMatch` for primary UMLS CUIs
and `skos:closeMatch` for alternative identifiers found in atoms or iKraph matches.

Assumptions:
 - COMBINI class URIs are: https://github.com/Tao-AI-group/COMBINI#{combini_label}
 - UMLS CUIs will be mapped to a resolvable URI using `https://identifiers.org/umls/{CUI}`
 - iKraph ids will be annotated as a literal with their biokdeid/id prefixed by `iKraph:` (no stable HTTP URI available)

"""
import csv
from pathlib import Path

UMLS_TSV = Path("/Users/drshika2/NodeNormalization/combo_umls_search_results.tsv")
ATOMS_TSV = Path("/Users/drshika2/NodeNormalization/combo_umls_atoms.tsv")
IKRAPH_TSV = Path("/Users/drshika2/NodeNormalization/combo_ikraph_semmed_results.tsv")
OUT_TTL = Path("/Users/drshika2/NodeNormalization/COMBO_skos_mappings.ttl")

SKOS_PREF = "http://www.w3.org/2004/02/skos/core#"

# helper
def combini_uri(label):
    # replace spaces in labels with underscores for safe fragment identifiers
    safe = label.replace(' ', '_')
    return f"<https://github.com/Tao-AI-group/COMBINI#{safe}>"

with open(OUT_TTL, "w", encoding="utf-8") as out:
    out.write("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n\n")

    # Primary UMLS exact matches
    with open(UMLS_TSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            comb = combini_uri(row["combini_label"])
            cui = row.get("umls_cui")
            if cui:
                # use identifiers.org pattern
                out.write(f"{comb} skos:exactMatch <https://identifiers.org/umls/{cui}> .\n")

    out.write("\n# iKraph / SemMedDB exact matches\n")
    # iKraph matches: map matched_id and matched_name
    with open(IKRAPH_TSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            if row.get("matched_id"):
                comb = combini_uri(row["combini_label"])
                src = row.get("source","")
                mid = row.get("matched_id")
                mname = row.get("matched_name","")
                if src == "iKraph":
                    # create a blank node literal mapping as closeMatch with label
                    out.write(f"{comb} skos:closeMatch [ a skos:Concept ; skos:prefLabel \"{mname}\" ; skos:note \"iKraph:{mid}\" ] .\n")
                else:
                    # SemMedDB -> map via CUI
                    out.write(f"{comb} skos:closeMatch <https://identifiers.org/umls/{mid}> .\n")

    out.write("\n# Alternative identifiers from UMLS atoms (closeMatch)\n")
    if ATOMS_TSV.exists():
        with open(ATOMS_TSV, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f, delimiter="\t")
            for row in r:
                comb = combini_uri(row["combini_label"])
                src = row.get("atom_source","")
                code = row.get("atom_code","")
                name = row.get("atom_name","")
                if src and code:
                    # Map common known sources to identifier patterns when possible
                    if src.upper() in ("MSH","MESH"):
                        out.write(f"{comb} skos:closeMatch <https://identifiers.org/mesh/{code}> .\n")
                    elif src.upper() in ("SNOMEDCT_US","SNOMEDCT"):
                        out.write(f"{comb} skos:closeMatch <https://identifiers.org/snomedct/{code}> .\n")
                    elif src.upper().startswith("RXNORM"):
                        out.write(f"{comb} skos:closeMatch <https://identifiers.org/rxnorm/{code}> .\n")
                    else:
                        # fallback: include as note-literal closeMatch
                        out.write(f"{comb} skos:closeMatch [ skos:prefLabel \"{name}\" ; skos:note \"{src}:{code}\" ] .\n")

print("Wrote SKOS mappings to", OUT_TTL)
