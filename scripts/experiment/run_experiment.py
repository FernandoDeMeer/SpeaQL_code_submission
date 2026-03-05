"""
Automated experiment runner for testing the Knowledge Assistant.

This script automates the usage of RDBExplorer and LLMHandler classes to simulate 
users asking the Knowledge Assistant questions from data/queries/ground_truth.jsonl.
Results are saved to logs in data/logs/.
"""

import os
import sys
import json
import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import sqlite3

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy import MetaData, select

from dotenv import load_dotenv

from dbkgexp.rdb_explorer import RDBExplorer, stable_row_id
from dbkgexp.llm_handler import LLMHandler
from dbkgexp.rel_node import RelationalNode


class ExperimentLogger:
    """Logger for experiment runs that saves results to JSONL files."""
    
    def __init__(self, logs_dir: str = "data/logs"):
        os.makedirs(logs_dir, exist_ok=True)
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = os.path.join(logs_dir, f'experiment_{self.session_id}.jsonl')
        self.results_file_path = os.path.join(logs_dir, f'results_{self.session_id}.jsonl')
        self.entries: List[Dict[str, Any]] = []
        
        # Log experiment start
        self.log_entry("experiment_start", {
            "timestamp": self.session_id,
            "script": "run_experiment.py"
        })
    
    def log_entry(self, event_type: str, data: Dict[str, Any]):
        """Log an entry with event type and data."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "data": data
        }
        self.entries.append(entry)
        print(f"[{event_type}] {json.dumps(data, default=str)[:200]}")
    
    def save(self):
        """Save all entries to JSONL file."""
        with open(self.log_file_path, 'w', encoding='utf-8') as f:
            for entry in self.entries:
                f.write(json.dumps(entry, default=str) + '\n')
        print(f"\n✓ Logs saved to: {self.log_file_path}")
    
    def append_result(self, result: Dict[str, Any]):
        """Append a result to the results file."""
        with open(self.results_file_path, 'a', encoding='utf-8') as f:
            json_str = json.dumps(result, default=str, indent=2)
            f.write(json_str)
        print(f"✓ Result saved for question: {result.get('question_id')}")


class ExperimentRunner:
    """Runs automated experiments using RDBExplorer and LLMHandler."""
    
    def __init__(
        self,
        db_url: str = "sqlite:///./streamlit/netflixdb.sqlite",
        api_key: Optional[str] = None,
        model: str = "openai/gpt-4o-mini",
        ground_truth_file: str = "data/queries/ground_truth.jsonl",
        initial_table: str = "SUB",
    ):
        """
        Initialize the experiment runner.
        
        Args:
            db_url: SQLAlchemy database URL
            api_key: OpenAI API key (uses OPENAI_API_KEY env var if None)
            model: LLM model to use
            ground_truth_file: Path to ground truth JSONL file
            initial_table: Initial table for RDBExplorer
        """
        # Load environment variables
        load_dotenv()
        
        # Initialize components
        self.engine = create_engine(db_url, echo=False)
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.model = model
        self.ground_truth_file = ground_truth_file
        self.initial_table = initial_table
        
        # Initialize logger
        self.logger = ExperimentLogger()
        
        # Verify database connection
        try:
            with self.engine.connect() as conn:
                pass
            self.logger.log_entry("database_connected", {"url": db_url})
        except Exception as e:
            self.logger.log_entry("error", {"type": "database_connection", "message": str(e)})
            raise
        
        # Initialize LLM handler
        try:
            self.llm_handler = LLMHandler(api_key=self.api_key, model=self.model)
            self.logger.log_entry("llm_initialized", {"model": self.model})
        except Exception as e:
            self.logger.log_entry("error", {"type": "llm_initialization", "message": str(e)})
            raise
    
    def load_ground_truth(self) -> List[Dict[str, Any]]:
        """Load ground truth questions from JSONL file."""
        questions = []
        
        if not os.path.exists(self.ground_truth_file):
            self.logger.log_entry("error", {
                "type": "file_not_found",
                "file": self.ground_truth_file
            })
            raise FileNotFoundError(f"Ground truth file not found: {self.ground_truth_file}")
        
        with open(self.ground_truth_file, 'r', encoding='utf-8') as f:
            # Handle both JSON array and JSONL formats
            content = f.read().strip()
            if content.startswith('['):
                # JSON array format
                questions = json.loads(content)
            else:
                # JSONL format - reset and read line by line
                f.seek(0)
                for line in f:
                    if line.strip():
                        questions.append(json.loads(line))
        
        self.logger.log_entry("questions_loaded", {"count": len(questions)})
        return questions
    
    def extract_search_key_from_question(self, question: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
        """
        Extract table name, column name, and search value from a question.
        
        Returns:
            Tuple of (table_name, column_name, search_value) or None if extraction fails
        """
        # Try to extract adsh from SQL query
        sql = question.get("sql_ground_truth", "")
        base_nl_query = question.get("base_nl_query", "")

        # Look for WHERE clauses with specific values
        if "adsh=" in sql:
            match = re.search(r"adsh='([^']+)'", sql)
            if match:
                return ("SUB", "adsh", match.group(1))
        
        else:
            # If the SQL query does not filter by the adsh, its value is mentioned in the base_nl_query
            #  We look for patterns like "0001166559-23-000004"
            match = re.search(r"(\d{10}-\d{2}-\d{6})", base_nl_query)
            if match:
                return ("SUB", "adsh", match.group(1))
        return None
    
    def build_knowledge_graph(
        self,
        table_name: str,
        column_name: str,
        search_value: str,
        depth: int = 2
    ) -> Optional[Dict[Tuple[str, str], RelationalNode]]:
        """
        Build knowledge graph for a given search query.
        
        Args:
            table_name: Initial table to search in
            column_name: Column to search
            search_value: Value to search for
            depth: Exploration depth for foreign keys
            
        Returns:
            Explored nodes dict or None if query returns no results
        """
        try:
            # Initialize RDBExplorer
            rdb_explorer = RDBExplorer(self.engine, table_name)
            
            # Query the database to find matching rows
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            table = metadata.tables[table_name]
            
            # Construct WHERE clause
            pattern = f"%{search_value}%" if search_value else "%"
            col = getattr(table.c, column_name)
            query = select(table).where(col.like(pattern)).limit(1)
            
            with self.engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
            
            if not rows:
                self.logger.log_entry("no_results", {
                    "table": table_name,
                    "column": column_name,
                    "search_value": search_value
                })
                return None
            
            # Get primary key from result
            row_data = rows[0]._mapping if hasattr(rows[0], '_mapping') else dict(rows[0])
            
            # Attempt to get primary key
            primary_key_cols = rdb_explorer._get_primary_key_safe(rdb_explorer.table)
            
            if primary_key_cols and primary_key_cols[0] in row_data:
                # Use actual primary key
                pk_value = row_data[primary_key_cols[0]]
                rdb_explorer.explore(
                    table_name=table_name,
                    primary_key=pk_value,
                    depth=depth
                )
            else:
                # Create synthetic node for table without primary key
                row_hash = stable_row_id(row_data)
                row_identifier = f"no_pk_{row_hash}"
                
                node = RelationalNode(
                    tableName=table_name,
                    primaryKey=str(row_identifier),
                    data=row_data,
                    foreignKeys=table.foreign_keys if table.foreign_keys else None,
                )
                rdb_explorer.explored_nodes[rdb_explorer._create_node_key(table_name, row_identifier)] = node
                
                # Explore from this node
                rdb_explorer.explore_from_existing_node(
                    table_name=table_name,
                    node_identifier=row_identifier,
                    depth=depth
                )
            
            self.logger.log_entry("knowledge_graph_built", {
                "nodes_count": len(rdb_explorer.explored_nodes),
                "depth": depth
            })
            
            return rdb_explorer.explored_nodes
            
        except Exception as e:
            self.logger.log_entry("error", {
                "type": "knowledge_graph_build",
                "message": str(e),
                "table": table_name,
                "column": column_name,
                "search_value": search_value
            })
            return None
    
    def process_question(
        self,
        question: Dict[str, Any],
        explored_nodes: Dict[Tuple[str, str], RelationalNode],
        n: int = 3
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Process a single question using the LLM handler.
        
        Args:
            question: Question dictionary from ground truth
            explored_nodes: Knowledge graph nodes
            n: Number of times to call the LLM handler (default 3)
            
        Returns:
            List of responses from LLM or None if processing fails
        """
        if not explored_nodes:
            return None
        
        try:
            query = question.get("app_nl_query", "")
            responses = []
            
            # Call LLM handler n times
            for i in range(n):
                validated_nodes, non_matching_nodes, summary = self.llm_handler(
                    graph_data=explored_nodes,
                    user_query=query
                )
                
                responses.append({
                    "call_number": i + 1,
                    "query": query,
                    "validated_nodes": [
                        (node.node_id, node.relevant_attributes) for node in validated_nodes
                    ],
                    "non_matching_nodes": [
                        (node.node_id, node.relevant_attributes) for node in non_matching_nodes
                    ],
                    "summary": summary.summary if hasattr(summary, 'summary') else str(summary)
                })
            
            return responses
            
        except Exception as e:
            self.logger.log_entry("error", {
                "type": "question_processing",
                "question_id": question.get("id"),
                "message": str(e)
            })
            return None
    
    def run_experiments(
        self,
        max_questions: Optional[int] = None,
        skip_errors: bool = True,
        question_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Run experiments on ground truth questions.
        
        Args:
            max_questions: Maximum number of questions to process (None for all)
            skip_errors: Continue on errors if True
            question_ids: List of specific question IDs to run (None for all)
            
        Returns:
            List of results
        """
        questions = self.load_ground_truth()
        results = []
        
        # Filter by question IDs if specified
        if question_ids:
            questions = [q for q in questions if q.get("id") in question_ids]        
        # Limit questions if specified
        if max_questions:
            questions = questions[:max_questions]
        
        for idx, question in enumerate(questions, 1):
            question_id = question.get("id", f"q{idx:03d}")
            
            try:
                # Extract search key from question
                search_info = self.extract_search_key_from_question(question)
                if not search_info:
                    self.logger.log_entry("warning", {
                        "type": "extraction_failed",
                        "question_id": question_id
                    })
                    if not skip_errors:
                        continue
                    results.append({
                        "question_id": question_id,
                        "status": "extraction_failed"
                    })
                    continue
                
                table_name, column_name, search_value = search_info
                
                # Build knowledge graph
                explored_nodes = self.build_knowledge_graph(
                    table_name,
                    column_name,
                    search_value,
                    depth=2
                )
                
                if not explored_nodes:
                    results.append({
                        "question_id": question_id,
                        "status": "no_graph"
                    })
                    continue
                
                # Process question with LLM
                responses = self.process_question(question, explored_nodes)
                
                if responses:
                    result = {
                        "question_id": question_id,
                        "status": "success",
                        "responses": responses,
                        "ground_truth": question.get("ground_truth")
                    }
                    results.append(result)
                    self.logger.append_result(result)
                else:
                    result = {
                        "question_id": question_id,
                        "status": "processing_failed"
                    }
                    results.append(result)
                    self.logger.append_result(result)
                    
            except Exception as e:
                self.logger.log_entry("error", {
                    "type": "question_processing",
                    "question_id": question_id,
                    "message": str(e)
                })
                if skip_errors:
                    result = {
                        "question_id": question_id,
                        "status": "error",
                        "error": str(e)
                    }
                    results.append(result)
                    self.logger.append_result(result)
                else:
                    raise
        
        self.logger.log_entry("experiment_complete", {
            "total_questions": len(questions),
            "successful": sum(1 for r in results if r.get("status") == "success"),
            "failed": sum(1 for r in results if r.get("status") != "success")
        })
        
        # Save logs
        self.logger.save()
        print(f"✓ All results saved to: {self.logger.results_file_path}")
        
        return results


def main():
    """Main entry point for the experiment runner."""
    parser = argparse.ArgumentParser(
        description="Run automated experiments on Knowledge Assistant using ground truth questions"
    )
    parser.add_argument(
        "--db-url",
        default="sqlite+pysqlite:///data/2024q1_notes/2024q1_notes.sqlite",
        help="SQLAlchemy database URL"
    )
    parser.add_argument(
        "--model",
        default="openai/gpt-4o-mini",
        help="LLM model to use"
    )
    parser.add_argument(
        "--ground-truth",
        default="data/queries/ground_truth.jsonl",
        help="Path to ground truth JSONL file"
    )
    parser.add_argument(
        "--max-questions",
        type=int,
        default=None,
        help="Maximum number of questions to process"
    )
    parser.add_argument(
        "--no-skip-errors",
        action="store_true",
        help="Stop on first error (default: continue on errors)"
    )
    parser.add_argument(
        "--question-ids",
        type=str,
        default=None,
        help="Comma-separated question IDs to run (for debugging purposes). Example: 'q001,q002,q003'"
    )
    
    args = parser.parse_args()

    # Optional local debug overrides (keep commented by default).
    # Prefer CLI flags, e.g.:
    # python scripts/experiment/run_experiment.py --question-ids q039 --model gpt-4.1
    # args.question_ids = 'q039'
    # args.model = "gpt-4.1"
    
    # Create and run experiment
    runner = ExperimentRunner(
        db_url=args.db_url,
        model=args.model,
        ground_truth_file=args.ground_truth,
    )
    
    # Parse question IDs if provided
    question_ids = None
    if args.question_ids:
        question_ids = [qid.strip() for qid in args.question_ids.split(",")]
    
    results = runner.run_experiments(
        max_questions=args.max_questions,
        skip_errors=not args.no_skip_errors,
        question_ids=question_ids
    )
    
    # Print summary
    print("\n" + "="*60)
    print("EXPERIMENT SUMMARY")
    print("="*60)
    successful = sum(1 for r in results if r.get("status") == "success")
    print(f"Total questions: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(results) - successful}")
    print("="*60)


if __name__ == "__main__":
    main()
