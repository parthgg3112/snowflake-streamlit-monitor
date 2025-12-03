import pandas as pd
from src.services import analyze_data

def test_failure_count():
    mock_df = pd.DataFrame({
        'PIPELINE_NAME': ['Sales', 'Finance', 'Sales', 'HR', 'Finance', 'HR', 'Logistics'],
        'STATUS': ['SUCCESS', 'FAILED', 'FAILED', 'SUCCESS', 'FAILED', 'FAILED', 'SUCCESS']
    })

    results = analyze_data(mock_df)

    row_a = results[results['PIPELINE_NAME'] == 'Sales'].iloc[0]
    assert row_a['FAILED_RUNS'] == 1
    print("Test case for Sales pipeline passed.")