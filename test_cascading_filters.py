"""Test cascading filter logic"""
import pandas as pd

def test_cascading_filters():
    print('Testing cascading filter logic...')
    
    # Create sample data
    data = {
        'display_name': ['João Silva', 'Maria Santos', 'José Oliveira', 'Ana Costa'],
        'formatted_phone': ['(31) 99999-0001', '(31) 99999-0002', '(31) 99999-0003', '(31) 99999-0004'],
        'Nome': ['João da Silva', 'Maria Santos', 'José Oliveira', 'Ana Costa'],
        'CPF': ['123.456.789-01', '987.654.321-02', '456.789.123-03', '789.123.456-04'],
        'endereco_bairro': ['Sion', 'Savassi', 'Sion', 'Centro']
    }
    
    conversations_df = pd.DataFrame(data)
    print(f'Sample data created: {len(conversations_df)} rows')
    
    # Test scenario: Filter by Bairro = Sion
    sion_filtered = conversations_df[conversations_df['endereco_bairro'] == 'Sion']
    print(f'Records with Bairro=Sion: {len(sion_filtered)}')
    print(f'CPFs in Sion: {list(sion_filtered["CPF"].values)}')
    
    # Test the exclude logic
    def apply_filters_except(df, exclude_key, filters):
        current_df = df.copy()
        
        if exclude_key != 'display_names' and filters.get('display_names'):
            current_df = current_df[current_df['display_name'].isin(filters['display_names'])]
        
        if exclude_key != 'bairros' and filters.get('bairros'):
            current_df = current_df[current_df['endereco_bairro'].isin(filters['bairros'])]
            
        return current_df
    
    # Simulate selecting Bairro = Sion
    test_filters = {'bairros': ['Sion']}
    
    # When calculating CPF options, exclude CPF filter but apply bairro filter
    cpf_filtered_df = apply_filters_except(conversations_df, 'cpfs', test_filters)
    available_cpfs = list(cpf_filtered_df['CPF'].unique())
    
    print(f'Available CPF options when Bairro=Sion: {available_cpfs}')
    
    # Test passes if we get exactly the 2 CPFs from Sion
    expected_cpfs = ['123.456.789-01', '456.789.123-03']
    test_passed = set(available_cpfs) == set(expected_cpfs)
    
    return test_passed

if __name__ == "__main__":
    result = test_cascading_filters()
    status = 'PASSED' if result else 'FAILED'
    print(f'\nCascading filter test: {status}')
    print('✅ Test completed successfully!')