import pytest
from unittest.mock import MagicMock

from backend.ml.embeddings import EmbeddingManager, EMBEDDING_MODEL_NAME


def test_model_metadata_matching(mocker):
    """
    Test that EmbeddingManager initializes correctly if the stored 
    pipeline_metadata matches the hardcoded EMBEDDING_MODEL_NAME.
    """
    # Mock external ML/DB dependencies to keep test fast and isolated
    mocker.patch('backend.ml.embeddings.chromadb.HttpClient')
    mocker.patch('backend.ml.embeddings.SentenceTransformer')
    
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    
    # Simulate DB returning the SAME model name (fetch returns tuple)
    # The first fetch is model_name, second is dimension. 
    # Our DB logic fetches name, then dim (if name exists).
    mock_conn.execute.return_value.fetchone.side_effect = [
        (EMBEDDING_MODEL_NAME,),  # match current
        (384,)                    # dimension
    ]
    
    manager = EmbeddingManager(db_engine=mock_engine)
    assert manager.model is not None


def test_model_metadata_mismatch(mocker):
    """
    Rule 7 Enforcement Check:
    If stored model in DB != current model context -> raise Exception and block initialization.
    """
    mocker.patch('backend.ml.embeddings.chromadb.HttpClient')
    mocker.patch('backend.ml.embeddings.SentenceTransformer')
    
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    
    # Simulate DB returning an older, DIFFERENT model name
    mock_conn.execute.return_value.fetchone.return_value = ("sentence-transformers/old-model-v1",)
    
    # Expect the pipeline to explicitly fail
    with pytest.raises(RuntimeError, match="Rule 7 Violation: Stored embedding model"):
        EmbeddingManager(db_engine=mock_engine)
