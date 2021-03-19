import pandas as pd
from sklearn.preprocessing import LabelEncoder
import os

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
    for i_in in range(0,lag_range) :
        if debug : print(i_in + 1)
        df_buf = df_out[col_in].shift(i_in + 1)
        col_buf = ['{}_{}_{}'.format(j_in, lag_name , i_in + 1) for j_in in df_buf.columns]
        df_buf.columns = col_buf
        df_out = pd.concat([df_out,df_buf] , axis = 1)
        lag_col += col_buf
    if drop_null : df_out = df_out[df_out[lag_col].notnull().min(axis = 1) != 0]
    return df_out