import os
from typing import Dict, Any, List, Tuple, Optional
from tqdm import tqdm
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, SkipValidation
from openai import OpenAI
import dspy
import tiktoken

from datetime import datetime
from dbkgexp.rel_node import RelationalNode

def num_tokens_from_string(string: str, encoding) -> int:
    return len(encoding.encode(string))

class RelevantNodeExtraction(BaseModel):
    """Model for relevant nodes and attributes."""

    node_id: str = Field(description="The node identifier in format (table:primary_key)")
    relevant_attributes: dict = Field(
        description="Dictionary of attribute name and value pairs relevant to the query"
    )
    relevance_reason: str = Field(
        description="Brief explanation of why this node is relevant"
    )


class QuerySummary(BaseModel):
    """Model for the final query summary."""

    summary: str = Field(
        description="Comprehensive summary based on query and relevant data"
    )


class NodeExtractor(dspy.Signature):
    """Extract relevant nodes and attributes from the knowledge graph based on user query."""

    graph_data: dict = dspy.InputField(
        desc="Dictionary representation of the knowledge graph with node data. Keys are node IDs (tableName:primaryKey). Values are dictionaries with 'data' and optional 'foreign_relationships'."
    )
    user_query: str = dspy.InputField(desc="Natural language query from the user")

    relevant_nodes: dict = dspy.OutputField(
        desc="Dictionary of relevant nodes with their attributes and reasons for relevance. Keys must be node IDs (tableName:primaryKey) existing in graph_data. Values are dictionaries with 'relevant_attributes' and 'relevance_reason'."
    )


class SummaryGenerator(dspy.Signature):
    """Generate a comprehensive summary based on relevant nodes and user query."""

    user_query: str = dspy.InputField(
        desc="Original natural language query from the user"
    )
    relevant_nodes: dict = dspy.InputField(
        desc="Dictionary representation of relevant nodes and attributes"
    )

    summary: str = dspy.OutputField(
        desc="Comprehensive summary answering the user's query based on the relevant nodes"
    )


class LLMHandler(dspy.Module):
    """
    dspy module that handles LLM operations for the Streamlit app, specifically for processing
    database knowledge graphs and generating responses to user queries.
    """

    def __init__(
        self, api_key: Optional[str] = None, model: str = "openai/gpt-4o-mini", llm_completion_margin: int = 4000
    ):
        """
        Initialize the LLM handler.

        Args:
            api_key: OpenAI API key. If None, will use OPENAI_API_KEY environment variable
            model: Model to use for LLM calls
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API key must be provided either as parameter or OPENAI_API_KEY environment variable"
            )

        # Normalize model name for tokenizers that may not recognize the "openai/" prefix
        normalized_model = model.split("/", 1)[1] if model.startswith("openai/") else model
        self.model = normalized_model

        # Configure DSPy
        try:
            dspy.configure(lm=dspy.LM(model, api_key=self.api_key))
        except RuntimeError as e:
            pass  # Ignore the error if already configured
        self.node_extractor = dspy.Predict(NodeExtractor)
        self.summary_generator = dspy.Predict(SummaryGenerator)

        self.encoding = tiktoken.encoding_for_model(self.model)
        self.context_window = self._get_model_context_window() - llm_completion_margin  # Leave margin for LLM response
        
        # Initialize cache for node embeddings
        self.node_embeddings: Dict[str, List[float]] = {}

    def _get_model_context_window(self) -> int:
        """Obtain model context window. Prefer OpenAI API; fall back to a safe default."""
        # Retrieved from https://platform.openai.com/docs/models/ (OpenAI's API does not provide this info directly)
        context_windows = {
            "gpt-4o": 128000,
            "gpt-4o-mini": 128000,
            "gpt-4-turbo": 128000,
        }
        return context_windows.get(self.model, 8192)
    
    def get_node_embeddings(
        self, graph_data: Dict[Tuple[str, str], RelationalNode]
    ) -> Dict[Tuple[str, str], List[float]]:
        """
        Generate embeddings for each node in the graph data using OpenAI embeddings.

        Args:
            graph_data: Dictionary of explored nodes from RDBExplorer

        Returns:
            Dictionary mapping node IDs to their embedding vectors.
        """
        openai_client = OpenAI(api_key=self.api_key)

        node_embeddings: Dict[Tuple[str, str], List[float]] = {}

        # Collect all uncached nodes to request embeddings in batch
        uncached_nodes = []
        node_text_map = {}  # Map node_id to node_text for later reference
        
        for (table_name, primary_key), node in graph_data.items():
            node_id = f"{table_name}:{primary_key}"

            # Reuse cached embedding if available
            if node_id in self.node_embeddings:
                node_embeddings[node_id] = self.node_embeddings[node_id]
            else:
                # Create a string representation of the node data for embedding
                node_text = str(node.data or {})
                uncached_nodes.append((table_name, primary_key, node_id))
                node_text_map[node_id] = node_text
        
        # Request embeddings for all uncached nodes in a single batch
        if uncached_nodes:
            texts_to_embed = [node_text_map[node_id] for _, _, node_id in uncached_nodes]
            
            # Partition texts by token count to respect OpenAI's 300K token limit per request
            batches = self._partition_texts_by_tokens(texts_to_embed)
            
            # Map from node_id to embedding for all batches
            all_embeddings = {}
                        
            # Process each batch
            batch_offset = 0
            for batch_idx, batch_texts in enumerate(
                tqdm(batches, total=len(batches), desc="Embedding nodes")
            ):
                response = openai_client.embeddings.create(
                    input=batch_texts,
                    model="text-embedding-3-small"
                )
                
                # Map embeddings back to node IDs for this batch
                for batch_local_idx, embedding_data in enumerate(response.data):
                    global_idx = batch_offset + batch_local_idx
                    if global_idx < len(uncached_nodes):
                        _, _, node_id = uncached_nodes[global_idx]
                        all_embeddings[node_id] = embedding_data.embedding
                
                batch_offset += len(batch_texts)
            
            # Process all results
            for idx, (table_name, primary_key, node_id) in enumerate(uncached_nodes):
                if node_id in all_embeddings:
                    embedding_vector = all_embeddings[node_id]
                    self.node_embeddings[node_id] = embedding_vector
                    node_embeddings[node_id] = embedding_vector

        return node_embeddings
    
    def _partition_texts_by_tokens(self, texts: List[str], max_tokens: int = 8000) -> List[List[str]]:
        """
        Partition texts into batches respecting the token limit per batch.
        Each text is cropped to the token limit before batching so a single large
        row cannot overflow the request.
        
        Args:
            texts: List of texts to embed
            max_tokens: Maximum tokens per batch
        
        Returns:
            List of text batches, each under the token limit
        """
        batches = []
        current_batch = []
        current_token_count = 0
        
        for text in texts:
            token_ids = self.encoding.encode(text)
            if len(token_ids) > max_tokens:
                token_ids = token_ids[:max_tokens]
                text = self.encoding.decode(token_ids)
            text_tokens = len(token_ids)
            
            # If adding this text exceeds limit, start a new batch
            if current_token_count + text_tokens > max_tokens and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_token_count = 0
            
            current_batch.append(text)
            current_token_count += text_tokens
        
        # Add remaining texts
        if current_batch:
            batches.append(current_batch)
        
        return batches if batches else [[]]  # Return at least one batch
    
    def calculate_similarities(
        self,
        query_embedding: List[float],
        node_embeddings: Dict[Tuple[str, str], List[float]],
    ) -> Dict[Tuple[str, str], float]:
        """
        Calculate cosine similarity between the query embedding and each node embedding.

        Args:
            query_embedding: Embedding vector for the user query
            node_embeddings: Dictionary mapping node IDs to their embedding vectors
        Returns:
            Dictionary mapping node IDs to their similarity scores with the query
        """
        similarity_scores = np.dot(np.array(list(node_embeddings.values())), 
                                   np.array(query_embedding)) / (np.linalg.norm(np.array(list(node_embeddings.values())), axis=1) * np.linalg.norm(np.array(query_embedding)))
        return {node_id: score for node_id, score in zip(node_embeddings.keys(), similarity_scores)}

    def get_embedding_similarities(
        self, graph_data: Dict[Tuple[str, str], RelationalNode], user_query: str
    ) -> Dict[Tuple[str, str], RelationalNode]:
        """
        Generate embeddings for the user query and calculate similarity scores with each node's embedding.

        Args:
            graph_data: Dictionary of explored nodes from RDBExplorer
            user_query: Natural language query from the user
        Returns:

            Dictionary mapping node IDs to their similarity scores with the query
            
        """

        node_embeddings = self.get_node_embeddings(graph_data)

        openai_client = OpenAI(api_key=self.api_key)
        query_response = openai_client.embeddings.create(
            input=user_query,
            model="text-embedding-3-small"
        )
        query_embedding = query_response.data[0].embedding

        similarity_scores = self.calculate_similarities(query_embedding, node_embeddings)
        
        return similarity_scores
    
    def filter_relevant_nodes(
        self, graph_data: Dict[Tuple[str, str], RelationalNode], user_query: str
    ) -> Dict[Tuple[str, str], RelationalNode]:
        """
        Filter the graph data to retain only nodes that are relevant to the user query
        based on embedding similarity scores.

        Args:
            graph_data: Dictionary of explored nodes from RDBExplorer
            user_query: Natural language query from the user

        Returns:
            Filtered dictionary of graph data with only relevant nodes
        """
        similarity_scores = self.get_embedding_similarities(graph_data, user_query)

        # Remove the node with the lowest similarity score until within context window
        sorted_nodes = sorted(
            similarity_scores.items(), key=lambda item: item[1]
        )  # Sort by similarity score (ascending)
        filtered_graph_data = graph_data.copy()
        total_tokens = num_tokens_from_string(
            str(self._graph_to_dict_representation(filtered_graph_data)), self.encoding
        )
        while total_tokens > self.context_window and sorted_nodes:
            # Remove the least similar node
            least_similar_node, _ = sorted_nodes.pop(0)
            table_name, primary_key = least_similar_node.split(":", 1)
            if (table_name, primary_key) in filtered_graph_data:
                # Estimate token impact of the node being removed and subtract instead of recomputing all tokens
                node_dict = {(table_name, primary_key): filtered_graph_data[(table_name, primary_key)]}
                removed_tokens = num_tokens_from_string(
                    str(self._graph_to_dict_representation(node_dict)), self.encoding
                )
                filtered_graph_data.pop((table_name, primary_key))
                total_tokens = max(0, total_tokens - removed_tokens)
        return filtered_graph_data

    def forward(
        self, graph_data: Dict[Tuple[str, str], RelationalNode], user_query: str
    ) -> Tuple[List[RelevantNodeExtraction], QuerySummary]:
        """
        Process the user query and extract relevant nodes, then generate a summary.

        Args:
            graph_data: Dictionary of explored nodes from RDBExplorer
            user_query: Natural language query from the user

        Returns:
            Tuple of (relevant_nodes, summary)
        """
        graph_data_str = self._graph_to_dict_representation(graph_data)

        # Count the number of tokens in the graph data; if larger than context window, filter the nodes
        total_tokens = num_tokens_from_string(str(graph_data_str), self.encoding)

        if total_tokens > self.context_window:
            graph_data_filtered = self.filter_relevant_nodes(
                graph_data=graph_data, user_query=user_query
            )

            # Step 1: Extract relevant nodes
            relevant_nodes = self.node_extractor(
                graph_data=graph_data_filtered, user_query=user_query
            )
        else:
            # Step 1: Extract relevant nodes
            relevant_nodes = self.node_extractor(
                graph_data=graph_data, user_query=user_query
            )

        # Step 2: Generate summary
        summary = self.summary_generator(
            user_query=user_query, relevant_nodes=relevant_nodes
        )

        relevant_nodes = self._parse_relevant_nodes(
            relevant_nodes.get("relevant_nodes")
        )  # Convert dict to RelevantNodeExtraction objects

        # Step 3: Authenticity guardrail
        validated_nodes, non_matching_nodes = self.authenticity_guardrail(
            graph_data, relevant_nodes
        )

        return (
            validated_nodes,
            non_matching_nodes,
            QuerySummary(summary=summary.get("summary")),
        )

    def _graph_to_dict_representation(
        self, explored_nodes: Dict[Tuple[str, str], RelationalNode]
    ) -> str:
        """
        Convert the explored nodes dictionary which has values such as ForeignKey into a dictionary representation
        with only string values to make it compatible with LLM processing.

        Args:
            explored_nodes: Dictionary of explored nodes from RDBExplorer

        Returns:
            Dictionary representation of the explored nodes with only string values.
        """
        graph_data = {}

        for (table_name, primary_key), node in explored_nodes.items():
            node_id = f"{table_name}:{primary_key}"

            # Extract foreign key relationships
            foreign_relationships = []
            if node.foreignKeys:
                for fk in node.foreignKeys:
                    fk_value = node.data.get(fk.parent.name) if node.data else None
                    if fk_value:
                        foreign_relationships.append(
                            {
                                "foreign_key_column": fk.parent.name,
                                "references_table": fk.column.table.name,
                                "references_value": fk_value,
                            }
                        )
            
            # Remove empty keys from the node's data
            node.data = {k: v for k, v in (node.data or {}).items() if v is not None and v != ''}

            if len(foreign_relationships) > 0:
                graph_data[node_id] = {
                    "data": node.data or {},
                    "foreign_relationships": foreign_relationships,
                }
            else:
                graph_data[node_id] = {
                    "data": node.data or {},
                }
                
        return graph_data

    def _parse_relevant_nodes(
        self, relevant_nodes_dict: dict
    ) -> List[RelevantNodeExtraction]:
        """
        Parse the dictionary response from the LLM into RelevantNodeExtraction objects.

        Args:
            relevant_nodes_dict: Dictionary from LLM

        Returns:
            List of RelevantNodeExtraction objects
        """
        relevant_nodes = []
        for node_id, attributes in relevant_nodes_dict.items():
            if isinstance(attributes, dict):
                relevant_node = RelevantNodeExtraction(
                    node_id=node_id,
                    relevant_attributes=attributes.get("relevant_attributes", {}),
                    relevance_reason=attributes.get("relevance_reason", ""),
                )
                relevant_nodes.append(relevant_node)
            else:
                raise ValueError(f"Invalid format for node {node_id}: {attributes}")

        return relevant_nodes

    def format_response_for_streamlit(
        self,
        validated_nodes: List[RelevantNodeExtraction],
        non_matching_nodes: List[RelevantNodeExtraction],
        summary: QuerySummary,
    ) -> Tuple[str, str, str]:
        """
        Format the LLM response for display in Streamlit.

        Args:
            validated_nodes: Nodes that passed authenticity guardrail
            non_matching_nodes: Nodes that failed authenticity guardrail
            summary: Generated summary

        Returns:
            Tuple of (formatted summary response, validated nodes output, non-matching nodes output)
        """
        summary_response = "{}".format(summary.summary)

        # Display validated nodes
        validated_nodes_output = ""
        if validated_nodes:
            validated_nodes_output = "**Validated Nodes:**\n"
            for node in validated_nodes:
                validated_nodes_output += f"- **Node ID:** {node.node_id}\n"
                validated_nodes_output += "  - **Relevant Attributes:**\n"
                for attr, value in node.relevant_attributes.items():
                    validated_nodes_output += f"    - {attr}: {value}\n"
                validated_nodes_output += (
                    f"  - **Relevance Reason:** {node.relevance_reason}\n\n"
                )

        # Display non-matching nodes
        non_matching_nodes_output = ""
        if non_matching_nodes:
            non_matching_nodes_output = "**Non-Matching Nodes:**\n"
            # Highlight that these nodes may have been hallucinated
            non_matching_nodes_output += " **⚠️ Warning:** *These nodes do not fully match the DB contents. The highlighted attribute values may belong to other related records or may have been hallucinated. Please manually check the actual values and compare to the summary.*\n\n"
            for node in non_matching_nodes:
                non_matching_nodes_output += f"- **Node ID:** {node.node_id}\n"
                non_matching_nodes_output += "  - **Relevant Attributes:**\n"
                for attr, value in node.relevant_attributes.items():
                    if isinstance(value, dict):
                        mismatch_type = value.get("mismatch_type")
                        if mismatch_type == "attribute_not_found":
                            # Attribute does not exist in the actual data
                            non_matching_nodes_output += f"    - **{attr}: LLM extracted '{value['llm_value']}' but the attribute does not exist in this record**\n"
                        elif mismatch_type == "different_record_exact_match":
                            # The attribute name and value pair exists in another record
                            non_matching_nodes_output += f"    - **{attr}: LLM extracted '{value['llm_value']}' but the attribute actually belongs to record '{value['found_in_node']}'**\n"
                        elif mismatch_type == "different_table_value_mismatch":
                            # The attribute exists on another table but with a different value
                            non_matching_nodes_output += f"    - **{attr}: LLM extracted '{value['llm_value']}' but the attribute exists in table '{value['found_in_table']}' with different values**\n"
                        elif mismatch_type == "value_mismatch":
                            # Show both LLM and actual values
                            non_matching_nodes_output += f"    - **{attr}: LLM extracted '{value['llm_value']}' but actual value is '{value['actual_value']}'**\n"
                    else:
                        non_matching_nodes_output += f"    - {attr}: {value}\n"
                non_matching_nodes_output += (
                    f"  - **Relevance Reason:** {node.relevance_reason}\n\n"
                )

        return summary_response, validated_nodes_output, non_matching_nodes_output

    def authenticity_guardrail(
        self, graph_data: Dict[str, Dict], relevant_nodes: List[RelevantNodeExtraction]
    ) -> Tuple[List[RelevantNodeExtraction], List[RelevantNodeExtraction]]:
        """
        This authenticity guardrail verifies that the relevant nodes extracted by the LLM
        from the graph data (of explored nodes form the RDBExplorer) have not been hallucinated.
        Every retrieved node must have a corresponding entry in the graph data and its attributes
        must match the actual values in the database, otherwise we remove the hallucinated nodes.

        Args:
            - graph_data: Dictionary representation of explored nodes (converted format)
            - relevant_nodes: List of relevant nodes extracted by the LLM

        Returns:
            - validated_nodes: List of relevant nodes that passed the authenticity guardrail check.
            If no nodes pass the check, an empty list is returned.

            - non_matching_nodes: List of nodes that did not pass the authenticity guardrail check.
            We will present these nodes to the user in the Streamlit app and tell them that they
            need to manually verify them, as they may have been hallucinated by the LLM.

        """
        validated_nodes = []
        non_matching_nodes = []

        for node in relevant_nodes:
            node_is_valid = True
            try:
                node_id = (node.node_id.split(":", 1))[0], (node.node_id.split(":", 1))[1]

                # Skip nodes that don't exist in the graph data
                if node_id not in graph_data:
                    # Check if the primary key exists in the graph data with a different table name
                    found_node, node_id = self.primary_key_exists_in_graph(graph_data, node_id[1])
                    node.node_id = node_id
                    if not found_node:
                        continue
            except:
                # If the node_id is just the primaryKey without a table name, we try to look for the primary key across all nodes of the graph data
                found_node, node_id = self.primary_key_exists_in_graph(graph_data, node.node_id)
                node.node_id = node_id
                if not found_node:
                    continue


            # Check if the attributes match the actual values in the database
            actual_attributes = graph_data[node_id].data
            for attr, value in node.relevant_attributes.items():
                if attr not in actual_attributes:
                    # If the attribute extracted does not exist in the actual data, we consider it a mismatch
                    node.relevant_attributes[attr] = {
                        "llm_value": value,
                        "mismatch_type": "attribute_not_found",
                    }

                    # First, check for exact matches (attribute name and value pair) across all other nodes
                    exact_match_found = False
                    for other_node_id, other_node_data in graph_data.items():
                        if other_node_id == node_id:
                            continue
                        if attr in other_node_data.data:
                            if str(other_node_data.data[attr]) == str(value):
                                # The attribute name and value pair exists in another record
                                node.relevant_attributes[attr] = {
                                    "llm_value": value,
                                    "mismatch_type": "different_record_exact_match",
                                    "found_in_node": other_node_id,
                                }
                                exact_match_found = True
                                break

                    # Only if no exact match is found, check for different tables with mismatched values
                    if not exact_match_found:
                        for other_node_id, other_node_data in graph_data.items():
                            if attr in other_node_data.data:
                                # The attribute exists on another table but with a different value
                                node.relevant_attributes[attr] = {
                                    "llm_value": value,
                                    "mismatch_type": "different_table_value_mismatch",
                                    "found_in_table": other_node_id[0],
                                }
                                break

                    node_is_valid = False
                elif str(actual_attributes[attr]) != str(value):
                    # If the value does not match the actual value, we consider it a mismatch
                    # Store both LLM and actual values for user to compare
                    node.relevant_attributes[attr] = {
                        "llm_value": value,
                        "actual_value": actual_attributes.get(attr),
                        "mismatch_type": "value_mismatch",
                    }
                    node_is_valid = False

                    # Exception handling for Datetime attributes
                    if type(actual_attributes[attr]) is datetime:
                        # Attempt to parse the LLM value as a datetime using multiple formats
                        datetime_formats = [
                            "%Y-%m-%d %H:%M:%S",  # 2023-01-15 14:30:00
                            "%Y-%m-%d %H:%M:%S.%f",  # 2023-01-15 14:30:00.123456
                            "%Y-%m-%d",  # 2023-01-15
                            "%m/%d/%Y",  # 01/15/2023
                            "%m/%d/%Y %H:%M:%S",  # 01/15/2023 14:30:00
                            "%d/%m/%Y",  # 15/01/2023
                            "%d/%m/%Y %H:%M:%S",  # 15/01/2023 14:30:00
                            "%Y-%m-%dT%H:%M:%S",  # 2023-01-15T14:30:00 (ISO format without timezone)
                            "%Y-%m-%dT%H:%M:%SZ",  # 2023-01-15T14:30:00Z (ISO format with Z)
                            "%Y-%m-%dT%H:%M:%S.%f",  # 2023-01-15T14:30:00.123456
                            "%Y-%m-%dT%H:%M:%S.%fZ",  # 2023-01-15T14:30:00.123456Z
                            "%B %d, %Y",  # January 15, 2023
                            "%B %d, %Y %H:%M:%S",  # January 15, 2023 14:30:00
                            "%b %d, %Y",  # Jan 15, 2023
                            "%b %d, %Y %H:%M:%S",  # Jan 15, 2023 14:30:00
                            "%d %B %Y",  # 15 January 2023
                            "%d %b %Y",  # 15 Jan 2023
                        ]

                        for date_format in datetime_formats:
                            try:
                                parsed_value = datetime.strptime(value, date_format)
                                if parsed_value == actual_attributes[attr]:
                                    node.relevant_attributes[attr] = value
                                    node_is_valid = True
                                    break
                            except ValueError:
                                continue

            if node_is_valid:
                validated_nodes.append(node)
            else:
                non_matching_nodes.append(node)

        # Return the list of relevant nodes that passed the authenticity guardrail check
        return validated_nodes, non_matching_nodes

    def primary_key_exists_in_graph(
        self, graph_data: Dict[str, Dict], primary_key: str
    ) -> bool:
        """
        Check if a primary key exists in the graph data.

        Args:
            graph_data: Dictionary representation of explored nodes (converted format)
            primary_key: Primary key to check
        Returns:
            True if primary key exists in graph data, False otherwise
        """
        graph_data_keys = pd.DataFrame(graph_data.keys(), columns=["table_name", "primary_key"])
        matching_key = graph_data_keys[graph_data_keys["primary_key"] == primary_key]
        if len(matching_key) > 0:
            table_name = matching_key.iloc[0]["table_name"]
            primary_key = matching_key.iloc[0]["primary_key"]
            node_id = (table_name, primary_key)
            found_node = True
            # Modify the node_id in the RelevantNodeExtraction object to include the table name
            node_id = (table_name, primary_key)
            return found_node, node_id
        return False, None
