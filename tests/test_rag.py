import pytest
from unittest.mock import MagicMock

from backend.ml.rag_query import RAGQueryEngine


def test_rag_hallucination_guard_threshold(mocker):
    """
    Rule 4: RAG empty context fallback.
    If the cosine similarity is below the 0.4 threshold, 
    do NOT call the HF inference API and immediately return 'Insufficient data'.
    """
    mocker.patch('backend.ml.rag_query.chromadb.HttpClient')
    mocker.patch('backend.ml.rag_query.SentenceTransformer')
    mock_hf_client_class = mocker.patch('backend.ml.rag_query.HFClient')
    
    mock_engine = MagicMock()
    engine = RAGQueryEngine(db_engine=mock_engine)
    
    # In ChromaDB cosine distance space: Distance = 1 - Cosine Similarity
    # So Distance of 0.8 => Similarity of 0.2
    # 0.2 < 0.4 (threshold)
    engine.collection.query.return_value = {
        'documents': [['Some totally irrelevant text about sports.']],
        'distances': [[0.8]],  
        'metadatas': [[{'url': 'http://irrelevant.com'}]]
    }
    
    response = engine.query("What are the quarterly profits for INFY.NS?")
    
    # Assert hard-fallback text
    assert response['answer'] == "Insufficient data"
    assert len(response['sources']) == 0
    
    # Crucial Rule 4 enforcement: Verify the LLM was NEVER prompted
    instance = mock_hf_client_class.return_value
    instance.query.assert_not_called()


def test_rag_successful_query(mocker):
    """
    If similarity >= threshold:
    Passes context to the LLM and properly returns answer + sources.
    """
    mocker.patch('backend.ml.rag_query.chromadb.HttpClient')
    mocker.patch('backend.ml.rag_query.SentenceTransformer')
    mock_hf_client = MagicMock()
    mocker.patch('backend.ml.rag_query.HFClient', return_value=mock_hf_client)
    
    mock_engine = MagicMock()
    engine = RAGQueryEngine(db_engine=mock_engine)
    
    # Distance of 0.1 => Similarity of 0.9 (Very high match)
    engine.collection.query.return_value = {
        'documents': [['TCS reported a record 10% YoY jump in net profits.']],
        'distances': [[0.1]],
        'metadatas': [[{
            'url': 'http://tcs.com/news', 
            'published_at': '2023-01-01T10:00:00', 
            'ticker': 'TCS.NS'
        }]]
    }
    
    mock_hf_client.query.return_value = [{"generated_text": "TCS reported a 10% YoY jump."}]
    
    response = engine.query("How did TCS do?")
    
    # Assert successful query parsing and source attachment
    assert response['answer'] == "TCS reported a 10% YoY jump."
    assert len(response['sources']) == 1
    assert response['sources'][0]['ticker'] == 'TCS.NS'
    assert response['sources'][0]['url'] == 'http://tcs.com/news'
    
    # LLM should have been called
    mock_hf_client.query.assert_called_once()
