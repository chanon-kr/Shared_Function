from sklearn.preprocessing import LabelEncoder
from sklearn.tree import export_text
import pandas as pd
import numpy as np
import os
from joblib import dump, load

def create_encoder(df_in , col_in, folder_in , debug = False) :
    if not os.path.exists(folder_in) : 
        if debug : print('Folder {} created'.format(folder_in))
        os.mkdir(folder_in)
    for i_in in col_in :
        en_encoder = LabelEncoder()
        en_encoder.fit(df_in[i_in].astype('str').astype('category'))
        dump(en_encoder, '{}/en__{}__.pkl'.format(folder_in , i_in))
        del en_encoder

def encode_col(df_in , col_in , folder_in , debug = False) :
    df_out = df_in.copy()
    for i_in in col_in :
        en_encoder = load('{}/en__{}__.pkl'.format(folder_in, i_in))
        all_class = list(en_encoder.classes_)
        df_out[i_in] = df_out[i_in].astype('str').astype('category').apply(lambda j_in : 
                                                             en_encoder.transform([j_in])[0] if j_in in all_class 
                                                             else len(all_class) )
        if debug : print(i_in , ':' , len(all_class) ,'Classes')
    return df_out

def decode_col(df_in , col_in , folder_in , debug = False) :
    df_out = df_in.copy()
    for i_in in col_in :
        en_encoder = load('{}/en__{}__.pkl'.format(folder_in, i_in))
        all_class = list(en_encoder.classes_)
        df_out[i_in] = df_out[i_in].astype('int').apply(lambda j_in : 
                                                             en_encoder.inverse_transform([j_in])[0] if j_in < len(all_class) 
                                                             else 'unknown' )
        if debug : print(i_in , ':' , len(all_class) ,'Classes')
    return df_out

def create_lag(df_in , col_in , lag_range , lag_name = 'lag' , drop_null = True, debug = False) :
    df_out = df_in.copy()
    lag_col = []
    if lag_range >= 0 : loop_range = range(0,lag_range)
    elif lag_range < 0 : loop_range = range(lag_range - 1, -1)
    for i_in in loop_range :
        if debug : print(i_in + 1)
        df_buf = df_out[col_in].shift(i_in + 1)
        col_buf = ['{}_{}_{}'.format(j_in, lag_name , str(i_in + 1).replace('-','m')) for j_in in df_buf.columns]
        df_buf.columns = col_buf
        df_out = pd.concat([df_out,df_buf] , axis = 1)
        lag_col += col_buf
    if drop_null : df_out = df_out[df_out[lag_col].notnull().min(axis = 1) != 0]
    return df_out

class lazy_treereason :
    """Explan Decision Tree Logic, Need More Optimize and Naming"""
    def __init__(self, model, feature_list, max_depth = 1000, spacing = 1, decimals = 4) :
        list_logic = export_text(model, feature_names=feature_list, max_depth = max_depth, spacing=spacing,decimals=decimals)
        list_logic = list_logic.split('\n')
        df_logic = pd.DataFrame({'logic' : list_logic})
        df_logic['depth'] = df_logic['logic'].apply(lambda x : len([c for c in x if c in '|']))
        df_logic = df_logic[df_logic['depth'] > 0]
        df_logic['begin'] = (df_logic['depth'] <= df_logic['depth'].shift(1)) | (df_logic['depth'] == 1)
        df_logic['end'] = (df_logic['depth'] >= df_logic['depth'].shift(-1)) | (df_logic['depth'] == max(df_logic['depth']))
        df_logic['end'].iloc[-1] = True
        df_logic['group'] = df_logic['begin'].cumsum()
        df_logic['logic'] = df_logic['logic'].apply(lambda x : str(x).split('|-')[-1][1:])
        df_logic['logic'] = np.where(df_logic['end'], df_logic['logic'], df_logic['logic'].str.split(' '))
        df_logic['feature'] = np.where(df_logic['end'], np.NaN, df_logic['logic'].apply(lambda x : x[0]))
        df_logic['operator'] = np.where(df_logic['end'], np.NaN, df_logic['logic'].apply(lambda x : x[1]))
        df_logic['value'] = np.where(df_logic['end'], np.NaN, df_logic['logic'].apply(lambda x : x[-1]))
        df_logic['output'] = np.where(df_logic['end'], df_logic['logic'], np.NaN)
        all_logic = pd.DataFrame()
        for i in df_logic['group'].unique() :
            buffer = all_logic.append(df_logic[df_logic['group'] == i])
            buffer = buffer[buffer['depth'] <= buffer[buffer['group'] == i]['depth'].max()]
            grouplist = list(buffer['group'].unique())
            grouplist.reverse()
            for j in grouplist :
                if j in list(buffer['group']) :
                    buffer = buffer[(~buffer['depth'].isin(buffer[buffer['group'] == j]['depth']))|(buffer['group'] >= j)]
            buffer['group'] = int(i)
            all_logic = all_logic.append(buffer)
        self.logic_str = list_logic[:]
        self.feature_list = feature_list[:]
        self.logic_frame = all_logic.reset_index(drop = True)
    
    def explain(self, x, logic = None, asframe = False , asstring = False):
        if logic == None : logic = self.logic_frame
        operator_dict = {'>' : lambda x,y : x > y,'>=' : lambda x,y : x >= y
                        ,'<' : lambda x,y : x < y,'<=' : lambda x,y : x <= y
                        ,'==' : lambda x,y : x == y}
        cal = logic.merge(pd.DataFrame({'actual' : x}).reset_index()
                          , how = 'inner' , left_on = 'feature' , right_on = 'index')
        cal['check'] = cal.apply(lambda x : operator_dict[x['operator']](float(x['actual']),float(x['value'])), axis = 1)
        cal = cal.groupby(['group']).agg({"check": 'sum',"feature": 'count'})
        cal = cal[cal['check'] == cal['feature']]
        if len(cal) != 1 :
            raise Exception("Error Found matching {} Cases.\nSuggest to try increase max_depth or decimals".format(len(cal)))
        elif asframe :
            return logic[logic['group'] == cal.index[0]]
        elif asstring :
            return str(list(logic[logic['group'] == cal.index[0]]['logic']))
        else :
            return list(logic[logic['group'] == cal.index[0]]['logic'])