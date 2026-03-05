import streamlit as st
from typing import Dict, Any, List, Tuple, Optional
import json
import re

from dbkgexp.llm_handler import LLMHandler
from dbkgexp.rel_node import RelationalNode


class StreamlitLLMHandler:
    """
    Streamlit-specific wrapper for LLM functionality that handles UI interactions
    and session state management.
    """

    def __init__(
        self, api_key: Optional[str] = None, model: str = "openai/gpt-4o-mini"
    ):
        """Initialize the Streamlit LLM handler."""
        self._init_session_state()
        self._init_llm_handler(api_key=api_key, model=model)
        self.logger = st.session_state.get("logger", None)

    def _init_session_state(self):
        """Initialize Streamlit session state for the LLM chat functionality."""
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        if "llm_handler" not in st.session_state:
            st.session_state.llm_handler = None
        if "current_relevant_nodes" not in st.session_state:
            st.session_state.current_relevant_nodes = []
        if "current_non_matching_nodes" not in st.session_state:
            st.session_state.current_non_matching_nodes = []

    def _init_llm_handler(
        self, api_key: Optional[str] = None, model: str = "openai/gpt-4o-mini"
    ):
        """Initialize the LLM handler with API key management."""
        if st.session_state.llm_handler is None:
            try:
                st.session_state.llm_handler = LLMHandler(api_key=api_key, model=model)
            except ValueError as e:
                st.error(f"Failed to initialize LLM: {str(e)}")
                st.info(
                    "Please set your OpenAI API key in the environment variable OPENAI_API_KEY"
                )
                return False

    def _process_user_query(
        self, user_query: str, explored_nodes: Dict[Tuple[str, str], RelationalNode]
    ):
        """
        Process a user query using the LLM handler.

        Args:
            user_query: User's natural language query
            explored_nodes: Current knowledge graph data
        """
        # Add user message to chat history
        st.session_state.chat_history.append({"role": "user", "content": user_query})

        # Show loading spinner
        with st.spinner("🧠 Analyzing your question..."):
            try:
                # relevant_nodes_filtered = st.session_state.llm_handler.filter_relevant_nodes(
                #     graph_data=explored_nodes,
                #     user_query=user_query,
                # )
                # Process query using LLM handler
                validated_nodes, non_matching_nodes, summary = (
                    st.session_state.llm_handler(
                        graph_data=explored_nodes, user_query=user_query
                    )
                )
                # Populate the relevant nodes in session state with both validated and non-matching nodes
                st.session_state.relevant_nodes = self.populate_relevant_nodes(
                    validated_nodes, non_matching_nodes
                )

                # Format response for display
                summary_response, validated_nodes_output, non_matching_nodes_output = (
                    st.session_state.llm_handler.format_response_for_streamlit(
                        validated_nodes=validated_nodes,
                        non_matching_nodes=non_matching_nodes,
                        summary=summary,
                    )
                )

                self.logger.log(key = "llm_response", value = {
                    "summary_response": summary.summary,
                    "validated_nodes": [(node.node_id, node.relevant_attributes) for node in validated_nodes],
                    "non_matching_nodes": [(node.node_id, node.relevant_attributes) for node in non_matching_nodes],
                })

                # Add assistant response to chat history
                st.session_state.chat_history.append(
                    {
                        "role": "assistant",
                        "summary": summary_response,
                        "validated_nodes": validated_nodes,
                        "non_matching_nodes": non_matching_nodes,
                    }
                )

                # Add validated nodes as guardrail messages if they exist
                if validated_nodes and validated_nodes_output:
                    st.session_state.chat_history.append(
                        {
                            "role": "valguardrail",
                            "validated_nodes_output": validated_nodes_output,
                        }
                    )

                # Add non-matching nodes as guardrail messages if they exist
                if non_matching_nodes and non_matching_nodes_output:
                    st.session_state.chat_history.append(
                        {
                            "role": "nonvalguardrail",
                            "non_matching_nodes_output": non_matching_nodes_output,
                        }
                    )

                # Update current relevant nodes for highlighting
                st.session_state.current_relevant_nodes = validated_nodes
                st.session_state.current_non_matching_nodes = non_matching_nodes

            except Exception as e:
                error_message = f"❌ Error processing query: {str(e)}"
                st.error(error_message)
                self.logger.log(key = "error_message", value = error_message)

                # Add error to chat history
                st.session_state.chat_history.append(
                    {"role": "error", "content": error_message}
                )

        # Rerun to update the display
        st.rerun()

    def render_chat_interface(
        self, explored_nodes: Dict[Tuple[str, str], RelationalNode]
    ):
        """
        Render the chat interface in Streamlit with sidebar integration.

        Args:
            explored_nodes: Current knowledge graph data
        """
        # Initialize chat history with structured format
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []

        # Create a container for chat messages with fixed height
        chat_container = st.container(height=1000)

        # Display chat messages
        with chat_container:
            for i, message in enumerate(st.session_state.chat_history):
                if message["role"] == "user":
                    # User messages with light blue background
                    with st.chat_message("user", avatar="👤"):
                        st.markdown(
                            f'<div class="user-message">{message["content"]}</div>',
                            unsafe_allow_html=True,
                        )
                elif message["role"] == "valguardrail":
                    # Validated guardrail messages with green background
                    with st.chat_message("valguardrail", avatar="✅"):
                        # Convert newlines to HTML line breaks and markdown bold to HTML bold
                        formatted_content = message["validated_nodes_output"].replace(
                            "\n", "<br>"
                        )
                        # Convert **text** to <strong>text</strong>
                        formatted_content = re.sub(
                            r"\*\*(.*?)\*\*", r"<strong>\1</strong>", formatted_content
                        )
                        st.markdown(
                            f'<div class="valguardrail-message">{formatted_content}</div>',
                            unsafe_allow_html=True,
                        )
                elif message["role"] == "nonvalguardrail":
                    # Non-matching guardrail messages with light red background
                    with st.chat_message("nonvalguardrail", avatar="⚠️"):
                        # Convert newlines to HTML line breaks and markdown bold to HTML bold
                        formatted_content = message[
                            "non_matching_nodes_output"
                        ].replace("\n", "<br>")
                        # Convert **text** to <strong>text</strong>
                        formatted_content = re.sub(
                            r"\*\*(.*?)\*\*", r"<strong>\1</strong>", formatted_content
                        )
                        # Convert *text* to <em>text</em>
                        formatted_content = re.sub(
                            r"\*(.*?)\*", r"<em>\1</em>", formatted_content
                        )
                        st.markdown(
                            f'<div class="nonvalguardrail-message">{formatted_content}</div>',
                            unsafe_allow_html=True,
                        )
                else:
                    # Assistant messages with light green background
                    with st.chat_message("assistant", avatar="🤖"):
                        if "summary" in message and message["summary"]:
                            st.markdown(
                                f'<div class="assistant-message">{message["summary"]}</div>',
                                unsafe_allow_html=True,
                            )
                        elif "content" in message and message["content"]:
                            st.markdown(
                                f'<div class="assistant-message">{message["content"]}</div>',
                                unsafe_allow_html=True,
                            )
                        else:
                            # Show red error box when no content is available
                            st.markdown(
                                f'<div class="error-message">❌ Please reformulate your query</div>',
                                unsafe_allow_html=True,
                            )

        # Chat input at the bottom
        prompt = st.chat_input("Ask a question to the LLM Assistant about the data.")

        # If user sends a message
        if prompt:
            self.logger.log(key = "user_query", value = prompt)
            self._process_user_query(prompt, explored_nodes)

        # Add clear history button
        if st.button("🗑️ Clear History"):
            st.session_state.chat_history = []
            st.session_state.current_relevant_nodes = []
            st.session_state.current_non_matching_nodes = []
            # Clear the sidebar
            with st.sidebar:
                st.empty()
            st.rerun()
    
    def populate_relevant_nodes(self, validated_nodes, non_matching_nodes) -> List[Dict[str, Any]]:
        """Populate relevant nodes from validated and non-matching nodes, we store node_ids only (as all relevant nodes are already in the explored nodes)"""
        relevant_nodes = []
        for extracted_node in validated_nodes:
            relevant_nodes.append(extracted_node.node_id)

        for extracted_node in non_matching_nodes:
            relevant_nodes.append(extracted_node.node_id)
        
        return relevant_nodes

    def export_analysis(self) -> Optional[Dict[str, Any]]:
        """
        Export the current analysis for download or external use.

        Returns:
            Dictionary containing the analysis data
        """
        if not st.session_state.chat_history:
            return None

        export_data = {"chat_history": st.session_state.chat_history}

        return export_data

    def render_export_options(self):
        """Render export options for the analysis."""
        if st.session_state.chat_history:
            st.subheader("💾 Export Analysis")

            export_data = self.export_analysis()
            if export_data:
                col1, col2 = st.columns(2)

                with col1:
                    # JSON download
                    json_str = json.dumps(export_data, indent=2, default=str)
                    st.download_button(
                        label="📄 Download as JSON",
                        data=json_str,
                        file_name="analysis_export.json",
                        mime="application/json",
                    )

                with col2:
                    # Text summary download
                    text_summary = self._generate_text_summary(export_data)
                    st.download_button(
                        label="📝 Download as Text",
                        data=text_summary,
                        file_name="analysis_summary.txt",
                        mime="text/plain",
                    )

    def _generate_text_summary(self, export_data: Dict[str, Any]) -> str:
        """Generate a text summary of the analysis."""
        summary_lines = ["Database Analysis Summary", "=" * 30, ""]

        # Add chat history
        for i, message in enumerate(export_data["chat_history"]):
            if message["role"] == "user":
                summary_lines.append(f"Q{i//2 + 1}: {message['content']}")
            else:
                summary_lines.append(f"A{i//2 + 1}: {message['content']}")
            summary_lines.append("")

        # Add relevant nodes summary
        if export_data["relevant_nodes"]:
            summary_lines.extend(["Relevant Data Points:", "-" * 20])

            for node in export_data["relevant_nodes"]:
                summary_lines.append(f"• {node['table_name']}:{node['primary_key']}")
                summary_lines.append(f"  Reason: {node['relevance_reason']}")
                summary_lines.append(
                    f"  Attributes: {', '.join(node['relevant_attributes'])}"
                )
                summary_lines.append("")

        return "\n".join(summary_lines)
