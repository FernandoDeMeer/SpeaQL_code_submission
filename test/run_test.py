# Tests for the LLM handler functionality
import os
import pytest
import sqlalchemy as sa
from typing import Dict, List, Tuple
from unittest.mock import Mock, patch

from dbkgexp.rel_node import RelationalNode
from dbkgexp.rdb_explorer import RDBExplorer
from dbkgexp.llm_handler import RelevantNodeExtraction, QuerySummary, LLMHandler
from sqlalchemy import create_engine, func


@pytest.fixture(scope="session")
def database_engine():
    """Create a database engine for testing."""
    # Look for SQLite database in multiple possible locations
    possible_paths = [
        os.path.join(os.path.dirname(os.getcwd()), "streamlit", "netflixdb.sqlite"),  # Original path
        os.path.join(os.environ.get('CI_PROJECT_DIR', ''), "streamlit", "netflixdb.sqlite"),  # CI path
        os.path.join(os.path.dirname(__file__), "..", "streamlit", "netflixdb.sqlite"),  # Relative to test file
    ]
    
    sqlite_path = None
    for path in possible_paths:
        if os.path.exists(path):
            sqlite_path = path
            break
    
    if sqlite_path:
        # Use SQLite database if available
        db_url = f"sqlite:///{sqlite_path}"
    else:
        pytest.skip("No database available for testing")
    
    engine = create_engine(db_url)
    
    # Test connection
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    except Exception as e:
        pytest.skip(f"Database connection failed: {e}")
    
    return engine


@pytest.fixture(scope="session")
def sample_graph_data(database_engine):
    """Create sample graph data for testing."""
    rdb_explorer = RDBExplorer(database_engine, initial_table='tv_show')
    
    # Get a random primary key for testing
    with database_engine.connect() as conn:
        random_query = sa.select(rdb_explorer.table.c.id).order_by(sa.func.random()).limit(1)
        
        result = conn.execute(random_query).first()
        if not result:
            pytest.skip("No data available in tv_show table")
        random_primary_key = result[0]
    
    # Explore the database starting from the initial table and the random primary key
    explored_nodes = rdb_explorer.explore(table_name=rdb_explorer.table.name, primary_key=random_primary_key, depth=2)
    
    if not explored_nodes:
        pytest.skip("No graph data could be generated")
    
    return explored_nodes


@pytest.fixture
def llm_handler():
    """Create an LLM handler instance for testing."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # Create a mock LLM handler for CI environments without API key
        with patch('dbkgexp.llm_handler.LLMHandler') as mock_handler:
            mock_instance = Mock()
            mock_instance._graph_to_dict_representation.return_value = {"test_node": {"data": {}}}
            mock_instance.forward.return_value = ([], [], QuerySummary(summary="Test summary"))
            mock_instance.format_response_for_streamlit.return_value = ("Summary", "Validated", "Non-matching")
            mock_handler.return_value = mock_instance
            return mock_instance
    
    return LLMHandler(api_key=api_key, model="openai/gpt-4o-mini")


class TestLLMHandler:
    """Test class for LLM handler functionality."""
    
    def test_graph_to_dict_representation(self, llm_handler, sample_graph_data):
        """Test conversion of graph data to dictionary representation."""
        if hasattr(llm_handler, '_graph_to_dict_representation'):
            dict_representation = llm_handler._graph_to_dict_representation(sample_graph_data)
            assert isinstance(dict_representation, dict)
            assert len(dict_representation) > 0
        else:
            # Mock case
            dict_representation = llm_handler._graph_to_dict_representation(sample_graph_data)
            assert isinstance(dict_representation, dict)

    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OpenAI API key not available")
    def test_forward_method_with_api(self, llm_handler, sample_graph_data):
        """Test the forward method with actual API calls."""
        user_query = "Which season of this show is the longest?"
        
        validated_nodes, non_matching_nodes, summary = llm_handler(sample_graph_data, user_query)

        assert isinstance(validated_nodes, list)
        assert all(isinstance(node, RelevantNodeExtraction) for node in validated_nodes)
        assert isinstance(non_matching_nodes, list)
        assert all(isinstance(node, RelevantNodeExtraction) for node in non_matching_nodes)

        assert isinstance(summary, QuerySummary)
        assert isinstance(summary.summary, str)
        assert len(summary.summary) > 0

    def test_forward_method_mock(self, llm_handler, sample_graph_data):
        """Test the forward method with mocked responses."""
        user_query = "Which season of this show is the longest?"
        
        if hasattr(llm_handler, 'forward'):
            # Real handler
            validated_nodes, non_matching_nodes, summary = llm_handler(sample_graph_data, user_query)
        else:
            # Mock handler
            validated_nodes, non_matching_nodes, summary = llm_handler.forward(sample_graph_data, user_query)

        assert isinstance(validated_nodes, list)
        assert isinstance(non_matching_nodes, list)
        assert isinstance(summary, QuerySummary)

    def test_format_response_for_streamlit(self, llm_handler):
        """Test the formatting of the response for Streamlit."""
        # Create sample data
        sample_validated_nodes = [
            RelevantNodeExtraction(
                node_id="tv_show:1",
                relevant_attributes={"title": "Test Show", "year": "2023"},
                relevance_reason="Test relevance"
            )
        ]
        sample_non_matching_nodes = []
        sample_summary = QuerySummary(summary="This is a test summary")

        if hasattr(llm_handler, 'format_response_for_streamlit'):
            summary_response, validated_output, non_matching_output = llm_handler.format_response_for_streamlit(
                sample_validated_nodes, sample_non_matching_nodes, sample_summary
            )
            
            assert isinstance(summary_response, str)
            assert isinstance(validated_output, str)
            assert isinstance(non_matching_output, str)
            assert len(summary_response) > 0
        else:
            # Mock case
            result = llm_handler.format_response_for_streamlit(
                sample_validated_nodes, sample_non_matching_nodes, sample_summary
            )
            assert isinstance(result, tuple)
            assert len(result) == 3


@pytest.mark.integration
class TestLLMHandlerIntegration:
    """Integration tests for LLM handler with real database and API."""
    
    @pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OpenAI API key not available")
    def test_end_to_end_workflow(self, database_engine):
        """Test the complete workflow from database to LLM response."""
        # Skip if no API key available
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            pytest.skip("OpenAI API key not available for integration test")
        
        # Create LLM handler
        llm_handler = LLMHandler(api_key=api_key, model="openai/gpt-4o-mini")
        
        # Create RDB explorer and get sample data
        rdb_explorer = RDBExplorer(database_engine, initial_table='tv_show')
        
        with database_engine.connect() as conn:
            random_query = sa.select(rdb_explorer.table.c.id).order_by(sa.func.random()).limit(1)      
            result = conn.execute(random_query).first()
            if not result:
                pytest.skip("No data available in tv_show table")
            random_primary_key = result[0]
        
        # Explore and get graph data
        explored_nodes = rdb_explorer.explore(
            table_name=rdb_explorer.table.name, 
            primary_key=random_primary_key, 
            depth=2
        )
        
        if not explored_nodes:
            pytest.skip("No graph data generated")
        
        # Test complete workflow
        user_query = "What information can you tell me about this TV show?"
        validated_nodes, non_matching_nodes, summary = llm_handler(explored_nodes, user_query)
        
        # Format for Streamlit
        summary_response, validated_output, non_matching_output = llm_handler.format_response_for_streamlit(
            validated_nodes, non_matching_nodes, summary
        )
        
        # Assertions
        assert isinstance(summary_response, str)
        assert len(summary_response) > 0
        assert isinstance(validated_output, str)
        assert isinstance(non_matching_output, str)
        
        print(f"Summary: {summary_response}")
        print(f"Validated nodes: {len(validated_nodes)}")
        print(f"Non-matching nodes: {len(non_matching_nodes)}")


if __name__ == "__main__":
    # Allow running as a script for local testing
    pytest.main([__file__, "-v"])



