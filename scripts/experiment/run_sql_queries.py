import sqlite3
import re
import json
import os
import sys
import importlib.util
import re

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from dbkgexp.rdb_explorer import stable_row_id

# load query functions
file_path_query = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/queries/sql_queries.py'))

spec_query = importlib.util.spec_from_file_location("sql_queries", file_path_query)

sql_queries = importlib.util.module_from_spec(spec_query)

spec_query.loader.exec_module(sql_queries)

DB_PATH = 'data/2024q1_notes/2024q1_notes.sqlite'
JSONL_OUT='data/queries/ground_truth.jsonl'


def extract_join_tables(sql: str) -> list|None:
    matches = [match.group(1)
            for match in re.finditer(
                r'\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*)',
                sql,
                re.IGNORECASE
            )
        ]
    return matches if len(matches) != 0 else None

def extract_main_table(sql):
    match = re.search(r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)',
                          sql,
                          re.IGNORECASE)
    return match.group(1) if match else None
    
def extract_table_name(sql:str) -> str|None:
    sql = sql.strip()
    if not sql.startswith('WITH'):
        main_match = extract_main_table(sql)
        # check for JOIN tables
        matches = extract_join_tables(sql)
        if matches is None:
            return main_match
        else:
            return main_match + '_' + '_'.join(t for t in matches) if main_match else None
    # get FROM statement after ')' -> when custom tables
    close_paren_index = sql.rfind(')')
    if close_paren_index == -1:
        return None
    
    main_query = sql[close_paren_index + 1:]
    main_match = extract_main_table(main_query)
    # check for JOIN tables
    matches = extract_join_tables(main_query)
    if matches is None:
        return main_match
    else:
        return main_match + '_' + '_'.join(t for t in matches) if main_match else None
    


# get functions
def collect_query_functions(module: object) -> list:
    funcs = []
    for name, obj in vars(module).items():
        if callable(obj) and re.fullmatch(r'query_string\d+', name):
            funcs.append((name,obj))

    # return number associated with function
    def id(item):
        name,_ = item
        return int(name.replace('query_string', '')
                   )
    
    return [fn for _,fn in sorted(funcs, key=id)]

# remove unnecessary spaces
def clean_sql(sql:str) -> str:
    return sql.strip().replace("\n"," ").replace("\t","")

# call database and run sql query
def run_sql(i:int,query:str) -> tuple:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    attributes = [d[0] for d in cursor.description]
    nr_data = len(results)
    conn.close()
    return results, attributes, nr_data

def main():
    query_functions = collect_query_functions(sql_queries)
    records=[]
    for i,query_func in enumerate(query_functions, start=1):
        current_json= {}
        id = f'q{i:03d}'
        sql = clean_sql(query_func()[2])
        result, attributes, nr_data = run_sql(i,sql)
        ground_truth = []
        for row in result:
            row_dict = (dict(zip(attributes, row)))
            # create hash id
            row_hash = [f'{extract_table_name(sql)}', f'no_pk_{stable_row_id(row)}']
            ground_truth.append([row_hash, row_dict])
        
        print(f'ID: {i}')
        current_json['id'] = id
        current_json['nr_data'] = nr_data
        current_json['base_nl_query'] = query_func()[1]
        current_json['app_nl_query'] = query_func()[0]
        current_json['sql_ground_truth'] = sql
        current_json['ground_truth'] = ground_truth

        records.append(current_json)

    with open(JSONL_OUT, 'w') as f:
        json.dump(records, f, indent=2)




if __name__=='__main__':
    main()
