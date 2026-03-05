import os
import json
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

DATA_DIR = "data/2024q1_notes"
METADATA_PATH = os.path.join(DATA_DIR, "notes-metadata.json")
DB_PATH = os.path.join(DATA_DIR, "2024q1_notes.sqlite")

# Map HTML/ReadMe types to SQLite types
TYPE_MAP = {
	"ALPHANUMERIC": "TEXT",
	"NUMERIC": "INTEGER",
	"DATE": "TEXT",
	"FLOAT": "REAL",
	"INTEGER": "INTEGER",
	"REAL": "REAL",
}

def parse_schema(metadata_path):
	"""
	Parse the notes-metadata.json file to extract the DB schema (including primary keys).
	
	:param metadata_path: Path to the notes-metadata.json file
	:return: Tuple of (schema dict, primary_keys dict)
		schema: {table_name: [(col_name, sql_type), ...], ...}
		primary_keys: {table_name: [pk_fields], ...}
	"""	
	schema = {}
	primary_keys = {}
	
	with open(metadata_path, 'r', encoding='utf-8') as f:
		metadata = json.load(f)
	
	for table_info in metadata.get('tables', []):
		url = table_info.get('url', '')
		# Extract table name from URL (e.g., 'sub.tsv' -> 'SUB')
		table_name = url.replace('.tsv', '').upper() if url else None
		
		if not table_name:
			continue
		
		table_schema = table_info.get('tableSchema', {})
		
		# Extract primary key(s)
		pk = table_schema.get('primaryKey')
		if pk:
			# primaryKey can be a string or a list
			if isinstance(pk, list):
				primary_keys[table_name] = pk
			else:
				primary_keys[table_name] = [pk]
		
		# Extract columns
		columns = []
		for col in table_schema.get('columns', []):
			col_name = col.get('name')
			if not col_name:
				continue
			
			# Get the datatype base and map to SQLite type
			datatype_info = col.get('datatype', {})
			base_type = datatype_info.get('base', 'string')
			
			# Map JSON Schema datatypes to SQLite types
			if base_type == 'decimal':
				sql_type = 'REAL'
			else:  # default to string and ALPHANUMERIC types
				sql_type = 'TEXT'
			
			columns.append((col_name, sql_type))
		
		schema[table_name] = columns
	
	return schema, primary_keys

def parse_fk_constraints(metadata_path):
	"""
	Parse the notes-metadata.json file to extract foreign key constraints.
	
	:param metadata_path: Path to the notes-metadata.json file
	:return: Dictionary of foreign key constraints
		{table_name: [{'foreign_fields': str, 'ref_table': str, 'ref_fields': str}, ...]}
	"""
	fk_constraints = {}
	
	with open(metadata_path, 'r', encoding='utf-8') as f:
		metadata = json.load(f)
	
	for table_info in metadata.get('tables', []):
		url = table_info.get('url', '')
		table_name = url.replace('.tsv', '').upper() if url else None
		
		if not table_name:
			continue
		
		table_schema = table_info.get('tableSchema', {})
		foreign_keys = table_schema.get('foreignKeys', [])
		
		if not foreign_keys:
			continue
		
		fk_constraints[table_name] = []
		
		for fk in foreign_keys:
			col_ref = fk.get('columnReference')
			reference = fk.get('reference', {})
			ref_resource = reference.get('resource', '')
			ref_col = reference.get('columnReference')
			
			# Extract referenced table name from resource URL
			# Resource can be like "sub.tsv" or "https://wwww.sec.gov/files2020q4.zip#path=tag.tsv"
			ref_table = None
			for possible_table in ['sub.tsv', 'tag.tsv', 'dim.tsv', 'ren.tsv', 'num.tsv', 'pre.tsv', 'cal.tsv', 'txt.tsv']:
				if possible_table in ref_resource:
					ref_table = possible_table.replace('.tsv', '').upper()
					break
			
			if not ref_table:
				continue
			
			# Handle both single column and composite foreign keys
			if isinstance(col_ref, list):
				foreign_fields = ','.join(col_ref)
			else:
				foreign_fields = col_ref
			
			if isinstance(ref_col, list):
				ref_fields = ','.join(ref_col)
			else:
				ref_fields = ref_col
			
			fk_constraints[table_name].append({
				'foreign_fields': foreign_fields,
				'ref_table': ref_table,
				'ref_fields': ref_fields
			})
	
	return fk_constraints

def create_tables(conn, schema, primary_keys=None, fk_constraints=None):
	"""Create tables with foreign key constraints.
	
	PRIMARY KEY constraints are added for tables that are referenced by other
	tables' foreign keys. 
	"""
	cur = conn.cursor()
	# Enable foreign key support
	cur.execute('PRAGMA foreign_keys = ON;')
	
	# Tables that MUST have PRIMARY KEY because they're referenced by FKs
	# SUB: referenced by REN, NUM, TXT, CAL
	# TAG: referenced by NUM, TXT, PRE, CAL
	# DIM: referenced by NUM, TXT
	# REN: referenced by PRE
	# NOTE: NUM is NOT included even though PRE might reference it in some datasets
	ref_tables = {'SUB', 'TAG', 'DIM', 'REN'}
	
	for table, columns in schema.items():
		col_defs = [f'"{name}" {ftype}' for name, ftype in columns]
		
		# Add primary key constraint for tables that are referenced by FKs
		if primary_keys and table in primary_keys and table in ref_tables:
			pk_fields = primary_keys[table]
			pk_clause = f'PRIMARY KEY ({",".join([f"\"{f}\"" for f in pk_fields])})'
			col_defs.append(pk_clause)
		
		# Add foreign key constraints if available
		if fk_constraints and table in fk_constraints:
			for fk in fk_constraints[table]:
				foreign_fields = fk['foreign_fields']
				ref_table = fk['ref_table']
				ref_fields = fk['ref_fields']
				
				# Handle composite keys
				foreign_cols = foreign_fields.split(',')
				ref_cols = ref_fields.split(',')
				
				fk_clause = f'FOREIGN KEY ({",".join([f"\"{c.strip()}\"" for c in foreign_cols])}) REFERENCES "{ref_table}" ({",".join([f"\"{c.strip()}\"" for c in ref_cols])})'
				col_defs.append(fk_clause)
		
		sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)});'
		cur.execute(sql)
	conn.commit()

def import_tsv_to_table(conn, table, columns, tsv_path, fk_constraints=None):
	"""Import TSV data to table, handling column name mismatches and FK constraint validation.
	
	This function:
	1. Loads the TSV file
	2. Maps TSV column names to schema column names
	3. Validates FK constraints and removes violating rows
	4. Inserts valid rows into the database
	
	:param conn: SQLite connection
	:param table: Table name
	:param columns: List of (column_name, sql_type) tuples
	:param tsv_path: Path to TSV file
	:param fk_constraints: Dictionary of foreign key constraints (optional)
	"""
	df = pd.read_csv(tsv_path, sep='\t', keep_default_na=False, low_memory=False)
	
	# Define column mappings for known TSV column name mismatches
	column_mappings = {
		'DIM': {'dimhash': 'dimh'}  # dimhash in TSV should be dimh in schema
	}
	
	# Apply column mappings if any exist for this table
	if table in column_mappings:
		rename_map = column_mappings[table]
		df = df.rename(columns=rename_map)
	
	# Get expected column names from schema
	expected_cols = [col for col, _ in columns]
	
	# Only keep columns that exist in schema
	cols_to_keep = [col for col in expected_cols if col in df.columns]
	df = df[cols_to_keep]
	
	# Convert uninformative dimension hash (0x00000000 = "no dimension") to NULL
	# This dimension hash appears in ~3.4M NUM records but provides no segmentation info.
	# By setting to NULL, FK constraints won't be enforced (standard SQL behavior).
	# This prevents massive graph hubs in downstream analysis while keeping all data.
	if 'dimh' in df.columns:
		null_count = (df['dimh'] == '0x00000000').sum()
		if null_count > 0:
			df.loc[df['dimh'] == '0x00000000', 'dimh'] = None
			print(f"  Set {null_count} uninformative dimension reference(s) to NULL (0x00000000)")
	
	# Check and remove rows that violate foreign key constraints
	if fk_constraints and table in fk_constraints:
		cur = conn.cursor()
		original_rows = len(df)
		
		for fk in fk_constraints[table]:
			foreign_fields = [f.strip() for f in fk['foreign_fields'].split(',')]
			ref_table = fk['ref_table']
			ref_fields = [f.strip() for f in fk['ref_fields'].split(',')]
			
			# Check if all foreign key columns exist in the dataframe
			if not all(col in df.columns for col in foreign_fields):
				print(f"  Warning: FK columns {foreign_fields} not in {table}")
				continue
			
			# For single-field foreign keys, use efficient filtering
			if len(foreign_fields) == 1 and len(ref_fields) == 1:
				foreign_col = foreign_fields[0]
				ref_col = ref_fields[0]
				
				# Get all valid referenced values
				query = f'SELECT DISTINCT "{ref_col}" FROM "{ref_table}"'
				try:
					cur.execute(query)
					valid_values = set()
					for row in cur.fetchall():
						valid_values.add(row[0] if row[0] is not None else None)
					
					# Keep only rows with valid foreign key references
					valid_mask = df[foreign_col].apply(lambda x: x in valid_values)
					removed = (~valid_mask).sum()
					
					if removed > 0:
						# If we would remove ALL rows, skip this constraint as it's likely incompatible
						if removed == len(df):
							print(f"  Warning: FK constraint {foreign_col} -> {ref_table}.{ref_col} would eliminate all rows - skipping constraint")
							print(f"    (Sample PRE values: {df[foreign_col].unique()[:5]}, Sample REF values: {list(valid_values)[:5]})")
						else:
							print(f"  Removing {removed} rows with invalid {foreign_col} -> {ref_table}.{ref_col}")
							df = df[valid_mask]
				except Exception as e:
					print(f"  Error validating FK constraint {foreign_col} -> {ref_table}.{ref_col}: {e}")
			else:
				# For composite keys, use a more manual approach
				query = f'SELECT DISTINCT {",".join([f"\"{f}\"" for f in ref_fields])} FROM "{ref_table}"'
				try:
					cur.execute(query)
					valid_tuples = set()
					for row in cur.fetchall():
						valid_tuples.add(row)
					
					# Find rows with valid composite foreign key references
					valid_mask = df[foreign_fields].apply(lambda x: tuple(v for v in x) in valid_tuples, axis=1)
					removed = (~valid_mask).sum()
					
					if removed > 0:
						# If we would remove ALL rows, skip this constraint as it's likely incompatible
						if removed == len(df):
							print(f"  Warning: FK constraint {foreign_fields} -> {ref_table}.{ref_fields} would eliminate all rows - skipping constraint")
							sample_pre = df[foreign_fields].iloc[0].tolist()
							sample_ref = list(valid_tuples)[0] if valid_tuples else None
							print(f"    (Sample data value: {sample_pre}, Sample ref value: {sample_ref})")
						else:
							print(f"  Removing {removed} rows with invalid composite FK {foreign_fields} -> {ref_table}.{ref_fields}")
							df = df[valid_mask]
				except Exception as e:
					print(f"  Error validating composite FK {foreign_fields} -> {ref_table}.{ref_fields}: {e}")
		
		if len(df) < original_rows:
			print(f"  Total: keeping {len(df)} of {original_rows} rows")
	
	# Insert the valid rows
	if len(df) > 0:
		df.to_sql(table, conn, if_exists='append', index=False)
		print(f"  Inserted {len(df)} rows")
	else:
		print(f"  No rows to insert")

def main():
	schema, primary_keys = parse_schema(METADATA_PATH)
	fk_constraints = parse_fk_constraints(METADATA_PATH)
	if os.path.exists(DB_PATH):
		os.remove(DB_PATH)
	conn = sqlite3.connect(DB_PATH)
	create_tables(conn, schema, primary_keys, fk_constraints)
			
	# Import tables in dependency order: referenced tables first
	# Build dependency graph: tables with no FKs or only FKs to themselves first
	imported = set()
	to_import = set(schema.keys())
	
	while to_import:
		# Find tables with all dependencies already imported
		ready = set()
		for table in to_import:
			if table not in fk_constraints:
				# No FK constraints, can import anytime
				ready.add(table)
			else:
				# Check if all referenced tables are already imported
				all_deps_met = True
				for fk in fk_constraints[table]:
					ref_table = fk['ref_table']
					if ref_table not in imported:
						all_deps_met = False
						break
				if all_deps_met:
					ready.add(table)
		
		if not ready:
			# No progress possible, import remaining tables in arbitrary order
			# This can happen if there are circular dependencies
			ready = to_import.copy()
		
		# Import ready tables
		for table in sorted(ready):
			tsv_file = os.path.join(DATA_DIR, f"{table.lower()}.tsv")
			if os.path.exists(tsv_file):
				print(f"Importing {tsv_file} into {table}...")
				columns = schema[table]
				import_tsv_to_table(conn, table, columns, tsv_file, fk_constraints)
				imported.add(table)
				to_import.discard(table)
				conn.commit()
			else:
				print(f"TSV file for {table} not found: {tsv_file}")
				to_import.discard(table)
	
	conn.close()
	print(f"Database created at {DB_PATH}")

if __name__ == "__main__":
	main()
