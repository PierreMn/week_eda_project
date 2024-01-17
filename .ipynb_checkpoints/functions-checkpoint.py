def calc_avg_step_duration(df):
    """Calculate average time spent on each step    
    Returns:
        DataFrame: avg_step_duration
    """
    df = df.sort_values(by=['client_id', 'date_time'])
    df = df.reset_index(drop=True)
    df['next_step_time'] = df.groupby(['client_id', 'visitor_id', 'visit_id'])['date_time'].shift(-1)
    df['step_duration'] = df['next_step_time'] - df['date_time']
    avg_step_duration = df.groupby('client_id')[['step_duration']].mean()
    avg_step_duration.columns=['avg_step_duration']
    return df, avg_step_duration


def clients_who_finished(df):
    """Calculate how many clients have finished the process (confirmed)."""
    
    clients_who_confirmed = df[df['process_step']=='confirm']
    clients_who_confirmed = clients_who_confirmed.drop_duplicates(subset='client_id')
    clients_who_confirmed['is_confirmed'] = 1
    clients_who_confirmed = clients_who_confirmed[['client_id', 'is_confirmed']]
    return clients_who_confirmed
  
def nr_of_visits(df):
    """Find out how many times each client visited the page"""
    
    df_client_visits = df.groupby('client_id')[['visit_id']].nunique()
    df_client_visits.columns=['no_of_visits']
    return df_client_visits

def nr_of_confirms(df):
    """How many time each client has confirmed?"""
    
    df_client_confirms = df[['client_id','process_step']][df['process_step']=='confirm'].groupby('client_id').count()
    df_client_confirms.columns = ['no_of_confirms']
    return df_client_confirms


# def detect_process_errors(df):
#     """Function that return the dataframe with added columns in order to conduct the error rate analysis"""
#     df = df.sort_values(['client_id','date_time'])
#     process_map = {'start':0, 'step_1':1, 'step_2':2, 'step_3':3, 'confirm':4}
#     df['process_step_id'] = df['process_step'].map(process_map)
#     df['previous_process_step_id'] = (df.sort_values(by=['client_id','date_time']).groupby('client_id')['process_step_id'].shift(1))
#     df['is_error'] = df['process_step_id'] < df['previous_process_step_id']
#     df_client_error = df[df['is_error']==True].groupby('client_id')['is_error'].agg([max,len])
#     df_client_error.columns = ['had_error','error_count']
#     return df_client_error

def detect_process_changes(df):
    """Function that return the dataframe with added columns in order to get information on the step changes (progress or step back) in the onboarding process"""
    df = df.sort_values(['client_id','date_time'])
    process_map = {'start':0, 'step_1':1, 'step_2':2, 'step_3':3, 'confirm':4}
    df['process_step_id'] = df['process_step'].map(process_map)
    df['previous_process_step_id'] = (df.sort_values(by=['client_id','date_time']).groupby('client_id')['process_step_id'].shift(1))
    df['is_error'] = df['process_step_id'] < df['previous_process_step_id']
    df['is_progress'] = df['process_step_id'] > df['previous_process_step_id']
    df_client_change = df[(df['is_error'] == True) | (df['is_progress'] == True)].groupby('client_id').agg({
    'is_error': ['max', 'sum'],
    'is_progress': ['max', 'sum']
})
    df_client_change.columns = ['had_error', 'error_count','had_progress','progress_count']
    return df, df_client_change

def data_summary(dfs):
    web_data_summary = dfs[0].join(dfs[1:])
    web_data_summary['had_error'] = web_data_summary['had_error'].fillna(False)
    web_data_summary['had_progress'] = web_data_summary['had_progress'].fillna(False)
    web_data_summary['progress_count'] = web_data_summary['progress_count'].fillna(0)
    web_data_summary = web_data_summary.fillna(0)
    web_data_summary['is_confirmed'] = web_data_summary['is_confirmed'].map({1:True, 0:False})
    web_data_summary[['error_count', 'progress_count', 'no_of_confirms']] = web_data_summary[['error_count', 'progress_count', 'no_of_confirms']].astype(int)
    return web_data_summary  


def nr_of_each_step(df):
    """Calculate how many time each client was repeating each step"""
    
    df_client_confirms = df[['client_id','process_step']][df['process_step']=='start'].groupby('client_id').count()
    df_client_confirms.columns = ['no_of_starts']
    
    df_client_confirms['no_of_step_1'] = df[['client_id','process_step']][df['process_step']=='step_1'].groupby('client_id').count()   
    df_client_confirms['no_of_step_2'] = df[['client_id','process_step']][df['process_step']=='step_2'].groupby('client_id').count()   
    df_client_confirms['no_of_step_3'] = df[['client_id','process_step']][df['process_step']=='step_3'].groupby('client_id').count()   
    df_client_confirms['no_of_confirms'] = df[['client_id','process_step']][df['process_step']=='confirm'].groupby('client_id').count()   
    return df_client_confirms