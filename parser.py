import csv
import json
import os
from typing import Dict

"""
Fields of the Imported CSV:

- The array BASE_COLS defines the first columns in each row with data mainly pertaining to the compound.
- A sequence of columns is repeated which creates the data for each protein in the row, which is stored in REPEAT_SUBJECT_COLS.
"""
REPEAT_SUBJECT_COLS = [
    'BindingDB Target Chain Sequence',
    'PDB ID(s) of Target Chain',
    'UniProt (SwissProt) Recommended Name of Target Chain',
    'UniProt (SwissProt) Entry Name of Target Chain',
    'UniProt (SwissProt) Primary ID of Target Chain',
    'UniProt (SwissProt) Secondary ID(s) of Target Chain',
    'UniProt (SwissProt) Alternative ID(s) of Target Chain',
    'UniProt (TrEMBL) Submitted Name of Target Chain',
    'UniProt (TrEMBL) Entry Name of Target Chain',
    'UniProt (TrEMBL) Primary ID of Target Chain',
    'UniProt (TrEMBL) Secondary ID(s) of Target Chain',
    'UniProt (TrEMBL) Alternative ID(s) of Target Chain'
]
BASE_COLS = [
    'BindingDB Reactant_set_id',
    'Ligand SMILES',
    'Ligand InChI',
    'Ligand InChI Key',
    'BindingDB MonomerID',
    'BindingDB Ligand Name',
    'Target Name Assigned by Curator or DataSource',
    'Target Source Organism According to Curator or DataSource',
    'Ki (nM)',
    'IC50 (nM)',
    'Kd (nM)',
    'EC50 (nM)',
    'kon (M-1-s-1)',
    'koff (s-1)',
    'pH',
    'Temp (C)',
    'Curation/DataSource',
    'Article DOI',
    'PMID',
    'PubChem AID',
    'Patent Number',
    'Authors',
    'Institution',
    'Link to Ligand in BindingDB',
    'Link to Target in BindingDB',
    'Link to Ligand-Target Pair in BindingDB',
    'Ligand HET ID in PDB',
    'PDB ID(s) for Ligand-Target Complex',
    'PubChem CID',
    'PubChem SID',
    'ChEBI ID of Ligand',
    'ChEMBL ID of Ligand',
    'DrugBank ID of Ligand',
    'IUPHAR_GRAC ID of Ligand',
    'KEGG ID of Ligand',
    'ZINC ID of Ligand',
    'Number of Protein Chains in Target (>1 implies a multichain complex)'
]


"""
COLUMN_DATA stores where each column should go in the actual document. Each key corresponds to a column name in the csv. For each of these, four fields must be defined:

- "location": defines the location of the column in the document, nested layers should be separated with a "."
- "type": The type of data in the column. Supported types include "int", "string", "split_comma" (ie. "a,b,c"), "split_semicolon" (ie "a;b;c"), "split_chembl" (ie "CHEMBL1CHEMBL2CHEMBL3"), "split_colon" (ie "a::b::c")
- "uniprot_type": This indicates whether the data is specifically applicable for "swissprot" or "trembl" documents, if it is applicable for both use "all"
- "relation": This simply should store a boolean of whether this field is in the relation of the document, as the relation is treated a bit differently in terms of merging
"""
COLUMN_DATA = {
    "BindingDB Reactant_set_id": {
      "location": "relation.bindingdb_set_id",
      "type": "int",
      "uniprot_type": "all",
      "relation": True
    },
    "Ligand SMILES": {
      "location": "object.smiles",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "Ligand InChI": {
      "location": "object.inchi",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "Ligand InChI Key": {
      "location": "object.inchikey",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "BindingDB MonomerID": {
      "location": "object.monomer_id",
      "type": "int",
      "uniprot_type": "all",
      "relation": False
    },
    "BindingDB Ligand Name": {
      "location": "object.name",
      "type": "split_colon",
      "uniprot_type": "all",
      "relation": False
    },
    "Target Name Assigned by Curator or DataSource": {
      "location": "subject.name",
      "type": "split_colon",
      "uniprot_type": "all",
      "relation": False
    },
    "Target Source Organism According to Curator or DataSource": {
      "location": "subject.organism",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "Ki (nM)": {
      "location": "relation.ki_nm",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "IC50 (nM)": {
      "location": "relation.ic50_nm",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Kd (nM)": {
      "location": "relation.kd_nm",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "EC50 (nM)": {
      "location": "relation.ec50_nm",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "kon (M-1-s-1)": {
      "location": "relation.kon",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "koff (s-1)": {
      "location": "relation.koff",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "pH": {
      "location": "relation.ph",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Temp (C)": {
      "location": "relation.temp_c",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Curation/DataSource": {
      "location": "relation.curation_datasource",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Article DOI": {
      "location": "relation.article_doi",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "PMID": {
      "location": "relation.pmid",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "PubChem AID": {
      "location": "relation.pubchem_aid",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Patent Number": {
      "location": "relation.patent_number",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Authors": {
      "location": "relation.authors",
      "type": "split_semicolon",
      "uniprot_type": "all",
      "relation": True
    },
    "Institution": {
      "location": "relation.institution",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Link to Ligand in BindingDB": {
      "location": "object.bindingdb_link",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "Link to Target in BindingDB": {
      "location": "subject.bindingdb_link",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "Link to Ligand-Target Pair in BindingDB": {
      "location": "relation.bindingdb_link",
      "type": "string",
      "uniprot_type": "all",
      "relation": True
    },
    "Ligand HET ID in PDB": {
      "location": "object.het_id_pdb",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "PDB ID(s) for Ligand-Target Complex": {
      "location": "relation.pdb",
      "type": "split_comma",
      "uniprot_type": "all",
      "relation": True
    },
    "PubChem CID": {
      "location": "object.pubchem_cid",
      "type": "int",
      "uniprot_type": "all",
      "relation": False
    },
    "PubChem SID": {
      "location": "object.pubchem_sid",
      "type": "int",
      "uniprot_type": "all",
      "relation": False
    },
    "ChEBI ID of Ligand": {
      "location": "object.chebi",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "ChEMBL ID of Ligand": {
      "location": "object.chembl",
      "type": "split_chembl",
      "uniprot_type": "all",
      "relation": False
    },
    "DrugBank ID of Ligand": {
      "location": "object.drugbank",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "IUPHAR_GRAC ID of Ligand": {
      "location": "object.iuphar_grac_id",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "KEGG ID of Ligand": {
      "location": "object.kegg",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "ZINC ID of Ligand": {
      "location": "object.zinc",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "Number of Protein Chains in Target (>1 implies a multichain complex)": {
      "location": "relation.num_protein_chains",
      "type": "int",
      "uniprot_type": "all",
      "relation": True
    },
    "BindingDB Target Chain Sequence": {
      "location": "subject.sequence",
      "type": "string",
      "uniprot_type": "all",
      "relation": False
    },
    "PDB ID(s) of Target Chain": {
      "location": "subject.pdb",
      "type": "split_comma",
      "uniprot_type": "all",
      "relation": False
    },
    "UniProt (SwissProt) Recommended Name of Target Chain": {
      "location": "subject.uniprot.fullname",
      "type": "string",
      "uniprot_type": "swissprot",
      "relation": False
    },
    "UniProt (SwissProt) Entry Name of Target Chain": {
      "location": "subject.uniprot.id",
      "type": "string",
      "uniprot_type": "swissprot",
      "relation": False
    },
    "UniProt (SwissProt) Primary ID of Target Chain": {
      "location": "subject.uniprot.accession",
      "type": "string",
      "uniprot_type": "swissprot",
      "relation": False
    },
    "UniProt (SwissProt) Secondary ID(s) of Target Chain": {
      "location": "subject.uniprot.secondary_accession",
      "type": "split_comma",
      "uniprot_type": "swissprot",
      "relation": False
    },
    "UniProt (SwissProt) Alternative ID(s) of Target Chain": {
      "location": "subject.uniprot.alternative_accession",
      "type": "split_comma",
      "uniprot_type": "swissprot",
      "relation": False
    },
    "UniProt (TrEMBL) Submitted Name of Target Chain": {
      "location": "subject.uniprot.fullname",
      "type": "string",
      "uniprot_type": "trembl",
      "relation": False
    },
    "UniProt (TrEMBL) Entry Name of Target Chain": {
      "location": "subject.uniprot.id",
      "type": "string",
      "uniprot_type": "trembl",
      "relation": False
    },
    "UniProt (TrEMBL) Primary ID of Target Chain": {
      "location": "subject.uniprot.accession",
      "type": "string",
      "uniprot_type": "trembl",
      "relation": False
    },
    "UniProt (TrEMBL) Secondary ID(s) of Target Chain": {
      "location": "subject.uniprot.secondary_accession",
      "type": "split_comma",
      "uniprot_type": "trembl",
      "relation": False
    },
    "UniProt (TrEMBL) Alternative ID(s) of Target Chain": {
      "location": "subject.uniprot.alternative_accession",
      "type": "split_comma",
      "uniprot_type": "trembl",
      "relation": False
    }
}


def append_field(doc: dict, key: str, value: any):
    key_path = COLUMN_DATA[key]['location']
    keys = key_path.split('.')
    key_ref = doc
    for i in keys[:len(keys)-1]:
        key_ref = key_ref[i]

    if COLUMN_DATA[key]['type'] == "split_comma" or COLUMN_DATA[key]['type'] == "split_semicolon" or COLUMN_DATA[key]['type'] == "split_chembl" or COLUMN_DATA[key]['type'] == "split_colon":
        if isinstance(key_ref[keys[-1]][0], list) and value not in key_ref[keys[-1]]:
            key_ref[keys[-1]].append(value)
        if not isinstance(key_ref[keys[-1]][0], list) and value != key_ref[keys[-1]]:
            key_ref[keys[-1]] = [key_ref[keys[-1]], value]
    else:
        if isinstance(key_ref[keys[-1]], list) and value not in key_ref[keys[-1]]:
            key_ref[keys[-1]].append(value)
        if not isinstance(key_ref[keys[-1]], list) and value != key_ref[keys[-1]]:
            key_ref[keys[-1]] = [key_ref[keys[-1]], value]


def set_field(doc: dict, key: str, value: any):
    key_path = COLUMN_DATA[key]['location']
    keys = key_path.split('.')
    key_ref = doc
    for i in keys[:len(keys)-1]:
        key_ref = key_ref[i]
    key_ref[keys[-1]] = value


def get_field(doc: dict, key: str):
    key_path = COLUMN_DATA[key]['location']
    keys = key_path.split('.')
    val = doc
    try:
        for i in keys:
            val = val[i]
        return val
    except KeyError:
        return None


def special_copy(base_dict):
    ret = {}
    ret['subject'] = base_dict['subject'].copy()
    ret['object'] = base_dict['object'].copy()
    ret['relation'] = base_dict['relation'].copy()
    ret['subject']['uniprot'] = {}
    return ret


def process_field(field_name: str, value: str):
    field_type = COLUMN_DATA[field_name]['type']
    if field_type == "int":
        return int(value)
    if field_type == "split_comma":
        return value.split(',')
    if field_type == "split_semicolon":
        return value.split('; ')
    if field_type == "split_colon":
        return value.split('::')
    if field_type == "split_chembl":
        return ['CHEMBL' + x for x in value.split('CHEMBL') if x != '']
    return value


def read_csv(file: str, delim: str):
    with open(file, encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file, delimiter=delim)
        first_line = True
        for row in reader:
            if len(row) == 0:
                continue
            if first_line:
                first_line = False
                continue
            else:
                base = {'object': {}, 'subject': {}, 'relation': {}}
                for j in range(37):
                    if row[j] is not None and row[j] != '' and row[j] != 'NULL':
                        val = process_field(BASE_COLS[j], row[j])
                        set_field(base, BASE_COLS[j], val)

                repeats = int(row[36])  # Number of Protein Chains in Target
                pos = 37
                for j in range(repeats):
                    info_1 = special_copy(base)
                    info_2 = special_copy(base)
                    info_1['subject']['uniprot']['type'] = 'swissprot'
                    info_2['subject']['uniprot']['type'] = 'trembl'
                    for k in range(12):
                        if row[pos] is not None and row[pos] != '':
                            val = process_field(
                                REPEAT_SUBJECT_COLS[k], row[pos])
                            if COLUMN_DATA[REPEAT_SUBJECT_COLS[k]]['uniprot_type'] == 'swissprot':
                                set_field(info_1, REPEAT_SUBJECT_COLS[k], val)
                            elif COLUMN_DATA[REPEAT_SUBJECT_COLS[k]]['uniprot_type'] == 'trembl':
                                set_field(info_2, REPEAT_SUBJECT_COLS[k], val)
                            else:
                                set_field(info_1, REPEAT_SUBJECT_COLS[k], val)
                                set_field(info_2, REPEAT_SUBJECT_COLS[k], val)
                        pos += 1
                    yield info_1
                    yield info_2


def arrayify(obj: Dict[str, any]):
    obj['relation'] = [obj['relation']]
    return obj


def merge(main: Dict[str, any], other: Dict[str, any]):
    for col in COLUMN_DATA:
        if COLUMN_DATA[col]['relation']:
            continue
        m_field = get_field(main, col)
        o_field = get_field(other, col)
        if o_field is None:
            continue

        if m_field is None:
            set_field(main, col, o_field)
        else:
            append_field(main, col, o_field)

    main['relation'].append(other['relation'])


def load_data(data_folder):
    docs = {}
    row_num = 0
    for row in read_csv(os.path.join(data_folder, './BindingDB_All.tsv'), '\t'):
        # print(row['subject']['uniprot'])
        try:
            entry_name = row['subject']['uniprot']['id']
            primary_id = row['subject']['uniprot']['accession']
        except KeyError:
            continue

        if entry_name is None or primary_id is None:
            continue

        if '_HUMAN' not in entry_name or primary_id == '':
            continue

        row['_id'] = f"{row['object']['monomer_id']}-{primary_id}"
        row['predicate'] = 'physically interacts with'

        if row['_id'] in docs:
            merge(docs[row['_id']], row)
        else:
            docs[row['_id']] = arrayify(row)

        # if row_num >= 1200000:
        #   break
        # if row_num % 50000 == 0:
        #   print(row_num)
        row_num += 1

    for doc_id in docs:
        yield docs[doc_id]


# def main():
#     from time import time

#     cnt = 0
#     c = 0
#     tim = time()

#     for row in load_data('./'):
#         if (row["_id"] == "13533-P00533"):
#             print('Writing record to file')
#             with open("record.json", "w") as r:
#                 json.dump(row, r, indent=2)

#     print(cnt)
#     print(c)
#     print("Time to execute: ", time()-tim)


# if __name__ == '__main__':
#     main()
