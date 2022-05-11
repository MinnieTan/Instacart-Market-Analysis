def add_groupby_feats(df, groupby_cols, agg_dict, df_new_cols):
    '''
    Input params:
        df - pandas dataframe, groupby_cols - list, agg_dict - dictionary, df_new_cols - list
    Output:
        df_new: pandas dataframe'''
    df_new = df.groupby(groupby_cols).agg(agg_dict)
    df_new.columns = df_new.columns.droplevel(0)
    df_new.reset_index(inplace = True)
    df_new.columns = groupby_cols + df_new_cols
    return df_new

def join_mul(df_left, df_right):
    '''
    Input params:
        df_left - pandas dataframe, df_right - pandas dataframe
    Output:
        df - pandas dataframe
    '''
    df = df_left.join(df_right, how = 'inner')
    return df

def merge_mul(merge_on, merge_way, df_left, df_right):
    '''
    Input params:
        df_left - pandas dataframe, df_right - pandas dataframe, merge_on = string or list, 
        merge_way - string
    Output:
        df - pandas dataframe'''
    df = df_left.merge(df_right, how = merge_way, on = merge_on)
    return df